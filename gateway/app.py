import os
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime

import httpx
from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from starlette.background import BackgroundTask

METADATA_URL = os.getenv("METADATA_URL", "http://metadata:8000")
API_KEYS = dict(item.split(":", 1) for item in os.getenv("API_KEYS", "dev-key-123:hussein").split(","))
RATE_LIMIT = int(os.getenv("RATE_LIMIT", "60"))
RATE_WINDOW = int(os.getenv("RATE_WINDOW", "60"))

request_times = defaultdict(list)
client = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global client
    client = httpx.AsyncClient(base_url=METADATA_URL, timeout=httpx.Timeout(300))
    yield
    await client.aclose()


def authenticate(response: Response, x_api_key: str | None = Header(default=None)):
    user = API_KEYS.get(x_api_key) if x_api_key else None
    if not user:
        raise HTTPException(401, "Missing or invalid API key", headers={"WWW-Authenticate": "ApiKey"})
    now = time.time()
    recent = [t for t in request_times[x_api_key] if now - t < RATE_WINDOW]
    if len(recent) >= RATE_LIMIT:
        retry_after = int(RATE_WINDOW - (now - recent[0])) + 1
        raise HTTPException(429, "Rate limit exceeded", headers={"Retry-After": str(retry_after)})
    recent.append(now)
    request_times[x_api_key] = recent
    response.headers["X-RateLimit-Limit"] = str(RATE_LIMIT)
    response.headers["X-RateLimit-Remaining"] = str(RATE_LIMIT - len(recent))
    return user


router = APIRouter(dependencies=[Depends(authenticate)])


class BucketIn(BaseModel):
    name: str
    redundancy: str = "replication"


def error_detail(r):
    try:
        return r.json().get("detail", r.text)
    except ValueError:
        return r.text


async def forward(method, path, **kwargs):
    try:
        r = await client.request(method, path, **kwargs)
    except httpx.HTTPError as e:
        raise HTTPException(502, f"Metadata service unreachable: {e}")
    if r.status_code >= 400:
        raise HTTPException(r.status_code, error_detail(r))
    return r.json()


def search_params(**filters):
    params = {}
    for name, value in filters.items():
        if value is None or value == []:
            continue
        params[name] = value.isoformat() if isinstance(value, datetime) else value
    return params


@router.post("/buckets", status_code=201)
async def create_bucket(body: BucketIn):
    return await forward("POST", f"/buckets/{body.name}", params={"redundancy": body.redundancy})


@router.get("/buckets")
async def list_buckets():
    return await forward("GET", "/buckets")


@router.put("/buckets/{bucket}/objects/{key}", status_code=201)
async def upload_object(bucket: str, key: str, request: Request, chunking: str = "fixed", tags: str = ""):
    return await forward(
        "PUT",
        f"/objects/{bucket}/{key}",
        params={"chunking": chunking, "tags": tags},
        content=request.stream(),
    )


@router.get("/buckets/{bucket}/objects/{key}")
async def download_object(bucket: str, key: str, byte_range: str | None = Header(default=None, alias="range")):
    headers = {"Range": byte_range} if byte_range else {}
    try:
        r = await client.send(client.build_request("GET", f"/objects/{bucket}/{key}", headers=headers), stream=True)
    except httpx.HTTPError as e:
        raise HTTPException(502, f"Metadata service unreachable: {e}")
    if r.status_code >= 400:
        await r.aread()
        await r.aclose()
        raise HTTPException(r.status_code, error_detail(r))
    passthrough = {
        name: value
        for name, value in r.headers.items()
        if name.lower() in ("content-length", "content-range", "accept-ranges")
    }
    passthrough["Content-Disposition"] = f'attachment; filename="{key}"'
    return StreamingResponse(
        r.aiter_raw(),
        status_code=r.status_code,
        headers=passthrough,
        media_type="application/octet-stream",
        background=BackgroundTask(r.aclose),
    )


@router.get("/buckets/{bucket}/objects/{key}/metadata")
async def object_metadata(bucket: str, key: str):
    return await forward("GET", f"/objects/{bucket}/{key}/metadata")


@router.delete("/buckets/{bucket}/objects/{key}")
async def delete_object(bucket: str, key: str):
    return await forward("DELETE", f"/objects/{bucket}/{key}")


@router.get("/buckets/{bucket}/objects")
async def list_objects(
    bucket: str,
    prefix: str | None = None,
    min_size: int | None = None,
    max_size: int | None = None,
    created_after: datetime | None = None,
    created_before: datetime | None = None,
    tag: list[str] = Query(default=[]),
    limit: int = 100,
):
    params = search_params(
        bucket=bucket, prefix=prefix, min_size=min_size, max_size=max_size,
        created_after=created_after, created_before=created_before, tag=tag, limit=limit,
    )
    return await forward("GET", "/objects", params=params)


@router.get("/search")
async def search_objects(
    bucket: str | None = None,
    prefix: str | None = None,
    min_size: int | None = None,
    max_size: int | None = None,
    created_after: datetime | None = None,
    created_before: datetime | None = None,
    tag: list[str] = Query(default=[]),
    limit: int = 100,
):
    params = search_params(
        bucket=bucket, prefix=prefix, min_size=min_size, max_size=max_size,
        created_after=created_after, created_before=created_before, tag=tag, limit=limit,
    )
    return await forward("GET", "/objects", params=params)


@router.get("/cluster/status")
async def cluster_status():
    return await forward("GET", "/cluster/status")


app = FastAPI(title="Object Storage Gateway", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


app.include_router(router)