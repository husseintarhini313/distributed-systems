import bisect
import hashlib


def position(value):
    return int(hashlib.sha256(value.encode()).hexdigest()[:16], 16)


class HashRing:
    def __init__(self, nodes, vnodes=100):
        self.nodes = set(nodes)
        self.ring = sorted((position(f"{node}#{i}"), node) for node in nodes for i in range(vnodes))
        self.keys = [k for k, _ in self.ring]

    def get_nodes(self, chunk_hash, count=1):
        if not self.ring:
            return []
        count = min(count, len(self.nodes))
        index = bisect.bisect(self.keys, int(chunk_hash[:16], 16))
        result = []
        while len(result) < count:
            node = self.ring[index % len(self.ring)][1]
            if node not in result:
                result.append(node)
            index += 1
        return result