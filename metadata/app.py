import hashlib
import os
import secrets
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

import db
from chunking import cdc_chunks, fixed_chunks
from ring import HashRing

FIXED_CHUNK_SIZE = int(os.getenv("FIXED_CHUNK_SIZE", 4 * 1024 * 1024))
CDC_MIN_SIZE = int(os.getenv("CDC_MIN_SIZE", 1 * 1024 * 1024))
CDC_AVG_SIZE = int(os.getenv("CDC_AVG_SIZE", 4 * 1024 * 1024))
CDC_MAX_SIZE = int(os.getenv("CDC_MAX_SIZE", 8 * 1024 * 1024))
REPLICAS = int(os.getenv("REPLICAS", "1"))
VNODES = int(os.getenv("VNODES", "100"))
HEARTBEAT_TIMEOUT = int(os.getenv("HEARTBEAT_TIMEOUT", "15"))

nodes = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    await run_in_threadpool(db.init_db)
    yield


app = FastAPI(title="Metadata Service", lifespan=lifespan)


class Heartbeat(BaseModel):
    node_id: str
    url: str
    free_space: int
    chunk_count: int


def alive_nodes():
    now = time.time()
    return sorted(n for n, info in nodes.items() if now - info["last_seen"] < HEARTBEAT_TIMEOUT)


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
        }
        for info in nodes.values()
    ]


def store_object(bucket, key, data, method):
    ring = HashRing(alive_nodes(), VNODES)
    if not ring.nodes:
        raise HTTPException(503, "No storage nodes available")

    start = time.time()
    pieces = split(data, method)
    chunking_seconds = round(time.time() - start, 2)

    hashes = []
    new_chunks = 0
    deduplicated = 0
    with httpx.Client(timeout=30) as client, db.connect() as conn:
        for piece in pieces:
            chunk_hash = hashlib.sha256(piece).hexdigest()
            hashes.append(chunk_hash)
            existing = conn.execute(
                "UPDATE chunks SET ref_count = ref_count + 1 WHERE hash = %s RETURNING hash",
                (chunk_hash,),
            ).fetchone()
            if existing:
                deduplicated += 1
                continue
            targets = ring.get_nodes(chunk_hash, REPLICAS)
            for node in targets:
                client.put(f"{nodes[node]['url']}/chunks/{chunk_hash}", content=piece).raise_for_status()
            conn.execute(
                "INSERT INTO chunks (hash, size, ref_count, nodes) VALUES (%s, %s, 1, %s)",
                (chunk_hash, len(piece), targets),
            )
            new_chunks += 1

        version = conn.execute(
            "SELECT COALESCE(MAX(version), 0) + 1 FROM objects WHERE bucket = %s AND key = %s",
            (bucket, key),
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO objects (bucket, key, version, size, chunking, chunk_hashes) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (bucket, key, version, len(data), method, hashes),
        )

    return {
        "bucket": bucket,
        "key": key,
        "version": version,
        "size": len(data),
        "chunking": method,
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
        "SELECT version, size, chunking, chunk_hashes, created_at FROM objects "
        "WHERE bucket = %s AND key = %s ORDER BY version DESC LIMIT 1",
        (bucket, key),
    ).fetchone()
    if not row:
        raise HTTPException(404, "Object not found")
    return row


@app.get("/objects/{bucket}/{key}/metadata")
def object_metadata(bucket: str, key: str):
    with db.connect() as conn:
        version, size, method, hashes, created_at = latest_version(conn, bucket, key)
    return {
        "bucket": bucket,
        "key": key,
        "version": version,
        "size": size,
        "chunking": method,
        "chunk_hashes": hashes,
        "created_at": created_at,
    }


@app.get("/objects/{bucket}/{key}")
def download_object(bucket: str, key: str):
    with db.connect() as conn, httpx.Client(timeout=30) as client:
        _, _, _, hashes, _ = latest_version(conn, bucket, key)
        parts = []
        for chunk_hash in hashes:
            holders = conn.execute("SELECT nodes FROM chunks WHERE hash = %s", (chunk_hash,)).fetchone()[0]
            for node in holders:
                if node not in nodes:
                    continue
                r = client.get(f"{nodes[node]['url']}/chunks/{chunk_hash}")
                if r.status_code == 200:
                    parts.append(r.content)
                    break
            else:
                raise HTTPException(503, f"Chunk {chunk_hash} is not available")
    return Response(content=b"".join(parts), media_type="application/octet-stream")


@app.delete("/objects/{bucket}/{key}")
def delete_object(bucket: str, key: str):
    removed = 0
    with db.connect() as conn, httpx.Client(timeout=30) as client:
        rows = conn.execute(
            "DELETE FROM objects WHERE bucket = %s AND key = %s RETURNING chunk_hashes",
            (bucket, key),
        ).fetchall()
        if not rows:
            raise HTTPException(404, "Object not found")
        for (hashes,) in rows:
            for chunk_hash in hashes:
                row = conn.execute(
                    "UPDATE chunks SET ref_count = ref_count - 1 WHERE hash = %s RETURNING ref_count, nodes",
                    (chunk_hash,),
                ).fetchone()
                if row and row[0] == 0:
                    for node in row[1]:
                        if node in nodes:
                            client.delete(f"{nodes[node]['url']}/chunks/{chunk_hash}")
                    conn.execute("DELETE FROM chunks WHERE hash = %s", (chunk_hash,))
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
            "SELECT node, COUNT(*) FROM chunks, unnest(nodes) AS node GROUP BY node ORDER BY node"
        ).fetchall()
    return {
        "unique_chunks": unique,
        "total_references": references,
        "logical_bytes": logical,
        "stored_bytes": stored,
        "dedup_ratio": round(logical / stored, 2) if stored else 0,
        "chunks_per_node": dict(per_node),
    }


def moved_percentage(keys, before, after, modulo=False):
    if modulo:
        moved = sum(1 for k in keys if before[int(k, 16) % len(before)] != after[int(k, 16) % len(after)])
    else:
        ring_before, ring_after = HashRing(before, VNODES), HashRing(after, VNODES)
        moved = sum(1 for k in keys if ring_before.get_nodes(k)[0] != ring_after.get_nodes(k)[0])
    return round(100 * moved / len(keys), 2)


@app.get("/placement/test")
def placement_test(add: str = "node-5", remove: str = "node-4", sample: int = 0):
    current = alive_nodes()
    if len(current) < 2:
        raise HTTPException(503, "Need at least 2 alive nodes")
    if sample > 0:
        keys = [secrets.token_hex(32) for _ in range(sample)]
    else:
        with db.connect() as conn:
            keys = [r[0] for r in conn.execute("SELECT hash FROM chunks").fetchall()]
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