import hashlib
import os
import threading
import time

import httpx
from fastapi import HTTPException
from psycopg.types.json import Jsonb

import db
import erasure
from ring import HashRing
from state import HEARTBEAT_TIMEOUT, alive_nodes, failed_nodes, nodes

REPLICAS = int(os.getenv("REPLICAS", "3"))
WRITE_QUORUM = int(os.getenv("WRITE_QUORUM", "2"))
READ_QUORUM = int(os.getenv("READ_QUORUM", "2"))
EC_K = int(os.getenv("EC_K", "4"))
EC_M = int(os.getenv("EC_M", "2"))
VNODES = int(os.getenv("VNODES", "100"))
SCRUB_INTERVAL = int(os.getenv("SCRUB_INTERVAL", "60"))

if WRITE_QUORUM + READ_QUORUM <= REPLICAS:
    raise RuntimeError("Invalid quorum: W + R must be greater than N")

client = httpx.Client(timeout=30)
repair_lock = threading.Lock()
repair_status = {
    "state": "idle",
    "failed_node": None,
    "chunks_total": 0,
    "chunks_done": 0,
    "fragments_repaired": 0,
    "started_at": None,
    "finished_at": None,
    "traffic": {},
}
scrub_status = {"last_run": None, "checked": 0, "corrupted": 0, "repaired": 0}


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def node_order(key):
    alive = alive_nodes()
    return HashRing(alive, VNODES).get_nodes(key, len(alive))


def put_fragment(node, data):
    fragment_hash = sha256(data)
    client.put(f"{nodes[node]['url']}/chunks/{fragment_hash}", content=data).raise_for_status()
    return fragment_hash


def fetch_fragment(node, fragment_hash):
    if node not in alive_nodes():
        return None
    try:
        r = client.get(f"{nodes[node]['url']}/chunks/{fragment_hash}")
    except httpx.HTTPError:
        return None
    return r.content if r.status_code == 200 else None


def check_fragment(node, fragment_hash):
    try:
        return client.get(f"{nodes[node]['url']}/chunks/{fragment_hash}").status_code
    except httpx.HTTPError:
        return None


def remove_fragment(node, fragment_hash):
    try:
        client.delete(f"{nodes[node]['url']}/chunks/{fragment_hash}")
    except httpx.HTTPError:
        pass


def delete_fragments(fragments):
    alive = alive_nodes()
    for f in fragments:
        if f["node"] in alive:
            remove_fragment(f["node"], f["hash"])


def write_replicated(data, chunk_hash):
    fragments = []
    for node in node_order(chunk_hash)[:REPLICAS]:
        try:
            put_fragment(node, data)
            fragments.append({"index": 0, "hash": chunk_hash, "node": node, "size": len(data)})
        except httpx.HTTPError:
            continue
    if len(fragments) < WRITE_QUORUM:
        raise HTTPException(503, f"Write quorum not reached ({len(fragments)}/{WRITE_QUORUM})")
    return fragments


def write_erasure(data, chunk_hash):
    order = node_order(chunk_hash)
    if not order:
        raise HTTPException(503, "No storage nodes available")
    fragments = []
    for index, share in enumerate(erasure.encode(data, EC_K, EC_M)):
        node = order[index % len(order)]
        try:
            fragment_hash = put_fragment(node, share)
            fragments.append({"index": index, "hash": fragment_hash, "node": node, "size": len(share)})
        except httpx.HTTPError:
            continue
    if len(fragments) < EC_K:
        raise HTTPException(503, f"Only {len(fragments)} fragments written, need {EC_K}")
    return fragments


def write_chunk(data, chunk_hash, scheme):
    if scheme == "ec":
        return write_erasure(data, chunk_hash)
    return write_replicated(data, chunk_hash)


def collect_shares(fragments):
    shares = {}
    for f in fragments:
        if len(shares) == EC_K:
            break
        data = fetch_fragment(f["node"], f["hash"])
        if data is not None:
            shares[f["index"]] = data
    return shares


def read_chunk(chunk_hash, scheme, size, fragments):
    if scheme == "ec":
        shares = collect_shares(fragments)
        if len(shares) < EC_K:
            raise HTTPException(503, f"Not enough fragments for chunk {chunk_hash}")
        return erasure.decode(shares, EC_K, EC_M, size)

    copies = []
    for f in fragments:
        data = fetch_fragment(f["node"], f["hash"])
        if data is not None:
            copies.append(data)
        if len(copies) == READ_QUORUM:
            break
    if not copies or len(copies) < min(READ_QUORUM, len(fragments)):
        raise HTTPException(503, f"Read quorum not reached for chunk {chunk_hash}")
    if any(c != copies[0] for c in copies):
        raise HTTPException(500, f"Replicas disagree for chunk {chunk_hash}")
    return copies[0]


def rebuild(chunk_hash, scheme, size, fragments, position, target=None):
    lost = fragments[position]
    others = [f for i, f in enumerate(fragments) if i != position]
    if target is None:
        holders = {f["node"] for f in others}
        order = node_order(chunk_hash)
        candidates = [n for n in order if n not in holders] or order
        if not candidates:
            return None, 0, 0
        target = candidates[0]

    if scheme == "ec":
        shares = collect_shares(others)
        if len(shares) < EC_K:
            return None, 0, 0
        data = erasure.decode(shares, EC_K, EC_M, size)
        share = erasure.encode(data, EC_K, EC_M)[lost["index"]]
        put_fragment(target, share)
        return {**lost, "node": target}, sum(len(s) for s in shares.values()), len(share)

    for f in others:
        data = fetch_fragment(f["node"], f["hash"])
        if data is not None:
            put_fragment(target, data)
            return {**lost, "node": target}, len(data), len(data)
    return None, 0, 0


def repair_node(node):
    with repair_lock, db.connect() as conn:
        rows = conn.execute(
            "SELECT hash, scheme, size, fragments FROM chunks WHERE fragments @> %s",
            (Jsonb([{"node": node}]),),
        ).fetchall()
        traffic = {s: {"bytes_read": 0, "bytes_written": 0, "fragments": 0} for s in ("replication", "ec")}
        repair_status.update(
            state="running", failed_node=node, chunks_total=len(rows), chunks_done=0,
            fragments_repaired=0, started_at=time.time(), finished_at=None, traffic=traffic,
        )
        for chunk_hash, scheme, size, fragments in rows:
            for i, f in enumerate(fragments):
                if f["node"] != node:
                    continue
                try:
                    new, read, written = rebuild(chunk_hash, scheme, size, fragments, i)
                except httpx.HTTPError:
                    continue
                if new:
                    fragments[i] = new
                    traffic[scheme]["bytes_read"] += read
                    traffic[scheme]["bytes_written"] += written
                    traffic[scheme]["fragments"] += 1
                    repair_status["fragments_repaired"] += 1
            conn.execute(
                "UPDATE chunks SET fragments = %s WHERE hash = %s AND scheme = %s",
                (Jsonb(fragments), chunk_hash, scheme),
            )
            conn.commit()
            repair_status["chunks_done"] += 1
        repair_status.update(state="finished", finished_at=time.time())


def monitor_loop():
    while True:
        now = time.time()
        for node, info in list(nodes.items()):
            dead = now - info["last_seen"] >= HEARTBEAT_TIMEOUT
            if dead and node not in failed_nodes:
                failed_nodes.add(node)
                print(f"[monitor] {node} missed its heartbeats, marked as failed, starting repair")
                try:
                    repair_node(node)
                except Exception as e:
                    print(f"[monitor] repair of {node} failed: {e}")
            elif not dead and node in failed_nodes:
                failed_nodes.discard(node)
                print(f"[monitor] {node} is back online")
        time.sleep(5)


def scrub_once():
    report = {"checked": 0, "corrupted": 0, "repaired": 0}
    with repair_lock, db.connect() as conn:
        alive = set(alive_nodes())
        rows = conn.execute("SELECT hash, scheme, size, fragments FROM chunks").fetchall()
        for chunk_hash, scheme, size, fragments in rows:
            changed = False
            for i, f in enumerate(fragments):
                if f["node"] not in alive:
                    continue
                report["checked"] += 1
                if check_fragment(f["node"], f["hash"]) not in (404, 500):
                    continue
                report["corrupted"] += 1
                print(f"[scrubber] bad fragment {f['hash'][:16]} on {f['node']}, repairing")
                remove_fragment(f["node"], f["hash"])
                try:
                    new, _, _ = rebuild(chunk_hash, scheme, size, fragments, i, target=f["node"])
                except httpx.HTTPError:
                    continue
                if new:
                    fragments[i] = new
                    changed = True
                    report["repaired"] += 1
            if changed:
                conn.execute(
                    "UPDATE chunks SET fragments = %s WHERE hash = %s AND scheme = %s",
                    (Jsonb(fragments), chunk_hash, scheme),
                )
    scrub_status.update(report, last_run=time.time())
    return report


def scrub_loop():
    while True:
        time.sleep(SCRUB_INTERVAL)
        try:
            scrub_once()
        except Exception as e:
            print(f"[scrubber] {e}")