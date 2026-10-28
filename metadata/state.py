import os
import time

HEARTBEAT_TIMEOUT = int(os.getenv("HEARTBEAT_TIMEOUT", "15"))

nodes = {}
failed_nodes = set()


def alive_nodes():
    now = time.time()
    return sorted(n for n, info in nodes.items() if now - info["last_seen"] < HEARTBEAT_TIMEOUT)