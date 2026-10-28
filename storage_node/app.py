import asyncio
import hashlib
import os
import re
import shutil
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Request, Response

NODE_ID = os.getenv("NODE_ID", "node-1")
DATA_DIR = os.getenv("DATA_DIR", "/data")
NODE_URL = os.getenv("NODE_URL", f"http://{NODE_ID}:8000")
METADATA_URL = os.getenv("METADATA_URL", "http://metadata:8000")
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "5"))

HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")
os.makedirs(DATA_DIR, exist_ok=True)


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def chunk_path(chunk_hash: str) -> str:
    return os.path.join(DATA_DIR, chunk_hash[:2], chunk_hash)


def validate_hash(chunk_hash: str):
    if not HASH_PATTERN.match(chunk_hash):
        raise HTTPException(400, "Invalid chunk hash: must be 64 lowercase hex characters")


def count_chunks() -> int:
    return sum(len(files) for _, _, files in os.walk(DATA_DIR))


async def send_heartbeats():
    async with httpx.AsyncClient(timeout=3) as client:
        while True:
            payload = {
                "node_id": NODE_ID,
                "url": NODE_URL,
                "free_space": shutil.disk_usage(DATA_DIR).free,
                "chunk_count": count_chunks(),
            }
            try:
                await client.post(f"{METADATA_URL}/heartbeat", json=payload)
            except httpx.HTTPError as e:
                print(f"[{NODE_ID}] Heartbeat failed: {e}")
            await asyncio.sleep(HEARTBEAT_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(send_heartbeats())
    yield
    task.cancel()


app = FastAPI(title=f"Storage Node {NODE_ID}", lifespan=lifespan)


@app.put("/chunks/{chunk_hash}", status_code=201)
async def put_chunk(chunk_hash: str, request: Request, response: Response):
    validate_hash(chunk_hash)
    data = await request.body()
    if sha256(data) != chunk_hash:
        raise HTTPException(400, "Content does not match the chunk hash")

    path = chunk_path(chunk_hash)
    if os.path.exists(path):
        response.status_code = 200
        return {"status": "already stored", "hash": chunk_hash, "node": NODE_ID}

    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = path + ".tmp"
    with open(tmp_path, "wb") as f:
        f.write(data)
    os.replace(tmp_path, path)  
    return {"status": "stored", "hash": chunk_hash, "size": len(data), "node": NODE_ID}


@app.get("/chunks/{chunk_hash}")
def get_chunk(chunk_hash: str):
    validate_hash(chunk_hash)
    path = chunk_path(chunk_hash)
    if not os.path.exists(path):
        raise HTTPException(404, "Chunk not found")

    with open(path, "rb") as f:
        data = f.read()
    if sha256(data) != chunk_hash:
        print(f"[{NODE_ID}] CORRUPTED chunk detected: {chunk_hash}")
        raise HTTPException(500, "Chunk is corrupted (hash mismatch)")
    return Response(content=data, media_type="application/octet-stream")


@app.head("/chunks/{chunk_hash}")
def head_chunk(chunk_hash: str):
    validate_hash(chunk_hash)
    path = chunk_path(chunk_hash)
    if not os.path.exists(path):
        raise HTTPException(404, "Chunk not found")
    return Response(headers={"Content-Length": str(os.path.getsize(path))})


@app.delete("/chunks/{chunk_hash}", status_code=204)
def delete_chunk(chunk_hash: str):
    validate_hash(chunk_hash)
    path = chunk_path(chunk_hash)
    if not os.path.exists(path):
        raise HTTPException(404, "Chunk not found")
    os.remove(path)
    return Response(status_code=204)


@app.get("/health")
def health():
    return {"node": NODE_ID, "status": "ok", "chunk_count": count_chunks()}