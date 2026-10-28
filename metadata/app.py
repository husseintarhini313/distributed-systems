import hashlib
import os
import secrets
import threading
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, Response
from psycopg.types.json import Jsonb
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

import db
import redundancy as rd
from chunking import cdc_chunks, fixed_chunks
from ring import HashRing
from state import HEARTBEAT_TIMEOUT, alive_nodes, failed_nodes, nodes

FIXED_CHUNK_SIZE = int(os.getenv("FIXED_CHUNK_SIZE", 4 * 1024 * 1024))
CDC_MIN_SIZE = int(os.getenv("CDC_MIN_SIZE", 1 * 1024 * 1024))
CDC_AVG_SIZE = int(os.getenv("CDC_AVG_SIZE", 4 * 1024 * 1024))
CDC_MAX_SIZE = int(os.getenv("CDC_MAX_SIZE", 8 * 1024 * 1024))
VNODES = int(os.getenv("VNODES", "100"))


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


def split(data, method):
    if method == "fixed":
        return fixed_chunks(data, FIXED_CHUNK_SIZE)
    if method == "cdc":
        return cdc_chunks(data, CDC_MIN_SIZE, CDC_AVG_SIZE, CDC_MAX_SIZE)
    raise HTTPException(400, "chunking must be 'fixed' or 'cdc'")


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


def store_object(bucket, key, data, method):
    with db.connect() as conn:
        row = conn.execute("SELECT redundancy FROM buckets WHERE name = %s", (bucket,)).fetchone()
        if not row:
            raise HTTPException(404, "Bucket not found")
        scheme = row[0]
        if not alive_nodes():
            raise HTTPException(503, "No storage nodes available")

        start = time.time()
        pieces = split(data, method)
        chunking_seconds = round(time.time() - start, 2)

        hashes = []
        new_chunks = 0
        deduplicated = 0
        for piece in pieces:
            chunk_hash = hashlib.sha256(piece).hexdigest()
            hashes.append(chunk_hash)
            existing = conn.execute(
                "UPDATE chunks SET ref_count = ref_count + 1 WHERE hash = %s AND scheme = %s RETURNING hash",
                (chunk_hash, scheme),
            ).fetchone()
            if existing:
                deduplicated += 1
                continue
            fragments = rd.write_chunk(piece, chunk_hash, scheme)
            conn.execute(
                "INSERT INTO chunks (hash, scheme, size, ref_count, fragments) VALUES (%s, %s, %s, 1, %s)",
                (chunk_hash, scheme, len(piece), Jsonb(fragments)),
            )
            new_chunks += 1

        version = conn.execute(
            "SELECT COALESCE(MAX(version), 0) + 1 FROM objects WHERE bucket = %s AND key = %s",
            (bucket, key),
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO objects (bucket, key, version, size, chunking, redundancy, chunk_hashes) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (bucket, key, version, len(data), method, scheme, hashes),
        )

    return {
        "bucket": bucket,
        "key": key,
        "version": version,
        "size": len(data),
        "chunking": method,
        "redundancy": scheme,
        "chunking_seconds": chunking_seconds,
        "total_chunks": len(hashes),
        "new_chunks": new_chunks,
        "deduplicated_chunks": deduplicated,
    }


@app.put("/objects/{bucket}/{key}", status_code=201)
async def upload_object(bucket: str, key: str, request: Request, chunking: str = "fixed"):
    data = await request.body()
    return await run_in_threadpool(store_object, bucket, key, data, chunking)


def latest_version(conn, bucket, key):
    row = conn.execute(
        "SELECT version, size, chunking, redundancy, chunk_hashes, created_at FROM objects "
        "WHERE bucket = %s AND key = %s ORDER BY version DESC LIMIT 1",
        (bucket, key),
    ).fetchone()
    if not row:
        raise HTTPException(404, "Object not found")
    return row


@app.get("/objects/{bucket}/{key}/metadata")
def object_metadata(bucket: str, key: str):
    with db.connect() as conn:
        version, size, method, scheme, hashes, created_at = latest_version(conn, bucket, key)
    return {
        "bucket": bucket,
        "key": key,
        "version": version,
        "size": size,
        "chunking": method,
        "redundancy": scheme,
        "chunk_hashes": hashes,
        "created_at": created_at,
    }


@app.get("/objects/{bucket}/{key}")
def download_object(bucket: str, key: str):
    with db.connect() as conn:
        _, _, _, scheme, hashes, _ = latest_version(conn, bucket, key)
        parts = []
        for chunk_hash in hashes:
            size, fragments = conn.execute(
                "SELECT size, fragments FROM chunks WHERE hash = %s AND scheme = %s",
                (chunk_hash, scheme),
            ).fetchone()
            parts.append(rd.read_chunk(chunk_hash, scheme, size, fragments))
    return Response(content=b"".join(parts), media_type="application/octet-stream")


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