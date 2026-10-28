import hashlib
import os
import re
import secrets
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from psycopg.types.json import Jsonb
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

import db
import redundancy as rd
from chunking import StreamSplitter
from ring import HashRing
from state import HEARTBEAT_TIMEOUT, alive_nodes, failed_nodes, nodes

FIXED_CHUNK_SIZE = int(os.getenv("FIXED_CHUNK_SIZE", 4 * 1024 * 1024))
CDC_MIN_SIZE = int(os.getenv("CDC_MIN_SIZE", 1 * 1024 * 1024))
CDC_AVG_SIZE = int(os.getenv("CDC_AVG_SIZE", 4 * 1024 * 1024))
CDC_MAX_SIZE = int(os.getenv("CDC_MAX_SIZE", 8 * 1024 * 1024))
VNODES = int(os.getenv("VNODES", "100"))
RANGE_PATTERN = re.compile(r"bytes=(\d*)-(\d*)")
OBJECT_FIELDS = ("bucket", "key", "version", "size", "chunking", "redundancy", "tags", "created_at")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await run_in_threadpool(db.init_db)
    threading.Thread(target=rd.monitor_loop, daemon=True).start()
    threading.Thread(target=rd.scrub_loop, daemon=True).start()
    yield


app = FastAPI(title="Metadata Service", lifespan=lifespan)


class Heartbeat(BaseModel):
    node_id: str
    url: str
    free_space: int
    chunk_count: int


def parse_tags(raw):
    tags = {}
    for item in filter(None, raw.split(",")):
        if "=" not in item:
            raise HTTPException(400, "tags must look like key=value,key2=value2")
        name, value = item.split("=", 1)
        tags[name.strip()] = value.strip()
    return tags


@app.post("/heartbeat")
def receive_heartbeat(hb: Heartbeat):
    nodes[hb.node_id] = {**hb.model_dump(), "last_seen": time.time()}
    return {"status": "ok"}


@app.get("/nodes")
def list_nodes():
    now = time.time()
    return [
        {
            **info,
            "seconds_since_heartbeat": round(now - info["last_seen"], 1),
            "alive": now - info["last_seen"] < HEARTBEAT_TIMEOUT,
            "failed": info["node_id"] in failed_nodes,
        }
        for info in sorted(nodes.values(), key=lambda n: n["node_id"])
    ]


@app.post("/buckets/{name}", status_code=201)
def create_bucket(name: str, redundancy: str = "replication"):
    if redundancy not in ("replication", "ec"):
        raise HTTPException(400, "redundancy must be 'replication' or 'ec'")
    with db.connect() as conn:
        row = conn.execute(
            "INSERT INTO buckets (name, redundancy) VALUES (%s, %s) "
            "ON CONFLICT (name) DO NOTHING RETURNING created_at",
            (name, redundancy),
        ).fetchone()
    if not row:
        raise HTTPException(409, "Bucket already exists")
    return {"bucket": name, "redundancy": redundancy, "created_at": row[0]}


@app.get("/buckets")
def list_buckets():
    with db.connect() as conn:
        rows = conn.execute("SELECT name, redundancy, created_at FROM buckets ORDER BY name").fetchall()
    return [{"bucket": n, "redundancy": r, "created_at": c} for n, r, c in rows]


def bucket_scheme(conn, bucket):
    row = conn.execute("SELECT redundancy FROM buckets WHERE name = %s", (bucket,)).fetchone()
    if not row:
        raise HTTPException(404, "Bucket not found")
    if not alive_nodes():
        raise HTTPException(503, "No storage nodes available")
    return row[0]


def save_chunk(conn, piece, scheme, result):
    chunk_hash = hashlib.sha256(piece).hexdigest()
    result["hashes"].append(chunk_hash)
    result["size"] += len(piece)
    existing = conn.execute(
        "UPDATE chunks SET ref_count = ref_count + 1 WHERE hash = %s AND scheme = %s RETURNING hash",
        (chunk_hash, scheme),
    ).fetchone()
    if existing:
        result["deduplicated"] += 1
        return
    fragments = rd.write_chunk(piece, chunk_hash, scheme)
    conn.execute(
        "INSERT INTO chunks (hash, scheme, size, ref_count, fragments) VALUES (%s, %s, %s, 1, %s)",
        (chunk_hash, scheme, len(piece), Jsonb(fragments)),
    )
    result["new"] += 1


def finish_object(conn, bucket, key, method, scheme, tags, result):
    version = conn.execute(
        "SELECT COALESCE(MAX(version), 0) + 1 FROM objects WHERE bucket = %s AND key = %s",
        (bucket, key),
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO objects (bucket, key, version, size, chunking, redundancy, chunk_hashes, tags) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (bucket, key, version, result["size"], method, scheme, result["hashes"], Jsonb(tags)),
    )
    conn.commit()
    return version


@app.put("/objects/{bucket}/{key}", status_code=201)
async def upload_object(bucket: str, key: str, request: Request, chunking: str = "fixed", tags: str = ""):
    if chunking not in ("fixed", "cdc"):
        raise HTTPException(400, "chunking must be 'fixed' or 'cdc'")
    tag_map = parse_tags(tags)
    splitter = StreamSplitter(chunking, FIXED_CHUNK_SIZE, CDC_MIN_SIZE, CDC_AVG_SIZE, CDC_MAX_SIZE)
    result = {"hashes": [], "size": 0, "new": 0, "deduplicated": 0}
    start = time.time()
    conn = await run_in_threadpool(db.connect)
    try:
        scheme = await run_in_threadpool(bucket_scheme, conn, bucket)
        async for part in request.stream():
            for piece in await run_in_threadpool(splitter.feed, part):
                await run_in_threadpool(save_chunk, conn, piece, scheme, result)
        for piece in await run_in_threadpool(splitter.finish):
            await run_in_threadpool(save_chunk, conn, piece, scheme, result)
        version = await run_in_threadpool(finish_object, conn, bucket, key, chunking, scheme, tag_map, result)
    except Exception:
        await run_in_threadpool(conn.rollback)
        raise
    finally:
        await run_in_threadpool(conn.close)

    return {
        "bucket": bucket,
        "key": key,
        "version": version,
        "size": result["size"],
        "chunking": chunking,
        "redundancy": scheme,
        "tags": tag_map,
        "upload_seconds": round(time.time() - start, 2),
        "total_chunks": len(result["hashes"]),
        "new_chunks": result["new"],
        "deduplicated_chunks": result["deduplicated"],
    }


def latest_version(conn, bucket, key):
    row = conn.execute(
        "SELECT version, size, chunking, redundancy, chunk_hashes, created_at, tags FROM objects "
        "WHERE bucket = %s AND key = %s ORDER BY version DESC LIMIT 1",
        (bucket, key),
    ).fetchone()
    if not row:
        raise HTTPException(404, "Object not found")
    return row


@app.get("/objects/{bucket}/{key}/metadata")
def object_metadata(bucket: str, key: str):
    with db.connect() as conn:
        version, size, method, scheme, hashes, created_at, tags = latest_version(conn, bucket, key)
    return {
        "bucket": bucket,
        "key": key,
        "version": version,
        "size": size,
        "chunking": method,
        "redundancy": scheme,
        "tags": tags,
        "chunk_hashes": hashes,
        "created_at": created_at,
    }


def parse_range(header, total):
    match = RANGE_PATTERN.fullmatch(header.strip())
    unsatisfiable = HTTPException(416, "Range not satisfiable", headers={"Content-Range": f"bytes */{total}"})
    if not match or match.group(1) == match.group(2) == "":
        raise unsatisfiable
    first, last = match.groups()
    if first == "":
        start = max(total - int(last), 0)
        end = total - 1
    else:
        start = int(first)
        end = min(int(last), total - 1) if last else total - 1
    if start > end or start >= total:
        raise unsatisfiable
    return start, end


@app.get("/objects/{bucket}/{key}")
def download_object(bucket: str, key: str, byte_range: str | None = Header(default=None, alias="range")):
    with db.connect() as conn:
        _, size, _, scheme, hashes, _, _ = latest_version(conn, bucket, key)
        chunks = []
        for chunk_hash in hashes:
            chunk_size, fragments = conn.execute(
                "SELECT size, fragments FROM chunks WHERE hash = %s AND scheme = %s",
                (chunk_hash, scheme),
            ).fetchone()
            chunks.append((chunk_hash, chunk_size, fragments))

    start, end = parse_range(byte_range, size) if byte_range else (0, size - 1)

    def generate():
        offset = 0
        for chunk_hash, chunk_size, fragments in chunks:
            chunk_start = offset
            offset += chunk_size
            if offset <= start or chunk_start > end:
                continue
            data = rd.read_chunk(chunk_hash, scheme, chunk_size, fragments)
            yield data[max(start - chunk_start, 0):end - chunk_start + 1]

    headers = {"Accept-Ranges": "bytes", "Content-Length": str(max(end - start + 1, 0))}
    status = 200
    if byte_range:
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"
        status = 206
    return StreamingResponse(generate(), status_code=status, headers=headers, media_type="application/octet-stream")


@app.delete("/objects/{bucket}/{key}")
def delete_object(bucket: str, key: str):
    removed = 0
    with db.connect() as conn:
        rows = conn.execute(
            "DELETE FROM objects WHERE bucket = %s AND key = %s RETURNING chunk_hashes, redundancy",
            (bucket, key),
        ).fetchall()
        if not rows:
            raise HTTPException(404, "Object not found")
        for hashes, scheme in rows:
            for chunk_hash in hashes:
                row = conn.execute(
                    "UPDATE chunks SET ref_count = ref_count - 1 WHERE hash = %s AND scheme = %s "
                    "RETURNING ref_count, fragments",
                    (chunk_hash, scheme),
                ).fetchone()
                if row and row[0] == 0:
                    rd.delete_fragments(row[1])
                    conn.execute("DELETE FROM chunks WHERE hash = %s AND scheme = %s", (chunk_hash, scheme))
                    removed += 1
    return {"deleted_versions": len(rows), "chunks_removed": removed}


@app.get("/objects")
def search_objects(
    bucket: str | None = None,
    prefix: str | None = None,
    min_size: int | None = None,
    max_size: int | None = None,
    created_after: datetime | None = None,
    created_before: datetime | None = None,
    tag: list[str] = Query(default=[]),
    limit: int = 100,
):
    conditions, params = [], []
    if bucket:
        conditions.append("bucket = %s")
        params.append(bucket)
    if prefix:
        conditions.append("key LIKE %s")
        params.append(prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%")
    if min_size is not None:
        conditions.append("size >= %s")
        params.append(min_size)
    if max_size is not None:
        conditions.append("size <= %s")
        params.append(max_size)
    if created_after:
        conditions.append("created_at >= %s")
        params.append(created_after)
    if created_before:
        conditions.append("created_at <= %s")
        params.append(created_before)
    for item in tag:
        if "=" not in item:
            raise HTTPException(400, "tag must look like key=value")
        name, value = item.split("=", 1)
        conditions.append("tags @> %s")
        params.append(Jsonb({name: value}))

    where = " AND ".join(conditions) if conditions else "TRUE"
    with db.connect() as conn:
        rows = conn.execute(
            "SELECT bucket, key, version, size, chunking, redundancy, tags, created_at FROM ("
            "SELECT DISTINCT ON (bucket, key) * FROM objects ORDER BY bucket, key, version DESC) latest "
            f"WHERE {where} ORDER BY bucket, key LIMIT %s",
            (*params, limit),
        ).fetchall()
    return [dict(zip(OBJECT_FIELDS, row)) for row in rows]


@app.get("/cluster/status")
def cluster_status():
    alive = alive_nodes()
    now = time.time()
    with db.connect() as conn:
        per_node = dict(conn.execute(
            "SELECT f->>'node', COUNT(*) FROM chunks, jsonb_array_elements(fragments) AS f GROUP BY 1"
        ).fetchall())
        degraded_chunks = set(conn.execute(
            "SELECT hash, scheme FROM chunks c WHERE "
            "EXISTS (SELECT 1 FROM jsonb_array_elements(c.fragments) f WHERE NOT (f->>'node' = ANY(%s::text[]))) "
            "OR (c.scheme = 'replication' AND jsonb_array_length(c.fragments) < %s) "
            "OR (c.scheme = 'ec' AND jsonb_array_length(c.fragments) < %s)",
            (alive, rd.REPLICAS, rd.EC_K + rd.EC_M),
        ).fetchall())
        latest = conn.execute(
            "SELECT DISTINCT ON (bucket, key) bucket, key, redundancy, chunk_hashes, size "
            "FROM objects ORDER BY bucket, key, version DESC"
        ).fetchall()

    degraded_objects = [
        {"bucket": b, "key": k}
        for b, k, scheme, hashes, _ in latest
        if any((h, scheme) in degraded_chunks for h in hashes)
    ]
    node_list = [
        {
            "node_id": node,
            "alive": node in alive,
            "failed": node in failed_nodes,
            "free_space": info["free_space"],
            "chunks_on_disk": info["chunk_count"],
            "fragments_assigned": per_node.get(node, 0),
            "seconds_since_heartbeat": round(now - info["last_seen"], 1),
        }
        for node, info in sorted(nodes.items())
    ]
    return {
        "total_nodes": len(nodes),
        "alive_nodes": len(alive),
        "free_space_bytes": sum(n["free_space"] for n in node_list if n["alive"]),
        "objects": len(latest),
        "logical_bytes": sum(row[4] for row in latest),
        "degraded_chunks": len(degraded_chunks),
        "degraded_objects_count": len(degraded_objects),
        "degraded_objects": degraded_objects[:100],
        "nodes": node_list,
        "repair": rd.repair_status,
    }


@app.get("/chunks/stats")
def chunk_stats():
    with db.connect() as conn:
        unique, references, stored = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(ref_count), 0), COALESCE(SUM(size), 0) FROM chunks"
        ).fetchone()
        logical = conn.execute("SELECT COALESCE(SUM(size), 0) FROM objects").fetchone()[0]
        per_node = conn.execute(
            "SELECT f->>'node', COUNT(*) FROM chunks, jsonb_array_elements(fragments) AS f GROUP BY 1 ORDER BY 1"
        ).fetchall()
    return {
        "unique_chunks": unique,
        "total_references": references,
        "logical_bytes": int(logical),
        "unique_chunk_bytes": int(stored),
        "dedup_ratio": round(float(logical) / float(stored), 2) if stored else 0,
        "fragments_per_node": dict(per_node),
    }


@app.get("/redundancy/stats")
def redundancy_stats():
    with db.connect() as conn:
        rows = conn.execute(
            "SELECT c.scheme, COUNT(*), SUM(c.size), SUM(f.total) FROM chunks c, "
            "LATERAL (SELECT SUM((x->>'size')::bigint) AS total FROM jsonb_array_elements(c.fragments) AS x) f "
            "GROUP BY c.scheme"
        ).fetchall()
    storage = {
        scheme: {
            "chunks": count,
            "logical_bytes": int(logical),
            "physical_bytes": int(physical),
            "overhead": round(float(physical) / float(logical), 2),
        }
        for scheme, count, logical, physical in rows
    }
    return {"storage": storage, "last_repair": rd.repair_status}


@app.get("/repair/status")
def repair_status():
    return rd.repair_status


@app.post("/scrub")
def run_scrub():
    return rd.scrub_once()


@app.get("/scrub/status")
def scrub_status():
    return rd.scrub_status


def moved_percentage(keys, before, after, modulo=False):
    if modulo:
        moved = sum(1 for k in keys if before[int(k, 16) % len(before)] != after[int(k, 16) % len(after)])
    else:
        ring_before, ring_after = HashRing(before, VNODES), HashRing(after, VNODES)
        moved = sum(1 for k in keys if ring_before.get_nodes(k)[0] != ring_after.get_nodes(k)[0])
    return round(100 * moved / len(keys), 2)


@app.get("/placement/test")
def placement_test(add: str = "node-7", remove: str = "node-6", sample: int = 0):
    current = alive_nodes()
    if len(current) < 2:
        raise HTTPException(503, "Need at least 2 alive nodes")
    if sample > 0:
        keys = [secrets.token_hex(32) for _ in range(sample)]
    else:
        with db.connect() as conn:
            keys = [r[0] for r in conn.execute("SELECT DISTINCT hash FROM chunks").fetchall()]
    if not keys:
        raise HTTPException(400, "No chunks stored yet, use ?sample=10000")

    added = sorted(current + [add])
    removed = [n for n in current if n != remove]
    return {
        "nodes": current,
        "chunks_tested": len(keys),
        "add_node": {
            "node": add,
            "consistent_hashing_moved_percent": moved_percentage(keys, current, added),
            "modulo_hashing_moved_percent": moved_percentage(keys, current, added, modulo=True),
            "ideal_percent": round(100 / len(added), 2),
        },
        "remove_node": {
            "node": remove,
            "consistent_hashing_moved_percent": moved_percentage(keys, current, removed),
            "modulo_hashing_moved_percent": moved_percentage(keys, current, removed, modulo=True),
            "ideal_percent": round(100 / len(current), 2),
        },
    }