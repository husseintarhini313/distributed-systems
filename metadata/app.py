import time

from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="Metadata Service")
nodes = {}


class Heartbeat(BaseModel):
    node_id: str
    url: str
    free_space: int
    chunk_count: int


@app.post("/heartbeat")
def receive_heartbeat(hb: Heartbeat):
    nodes[hb.node_id] = {**hb.model_dump(), "last_seen": time.time()}
    return {"status": "ok"}


@app.get("/nodes")
def list_nodes():
    now = time.time()
    return [
        {**node, "seconds_since_heartbeat": round(now - node["last_seen"], 1)}
        for node in nodes.values()
    ]