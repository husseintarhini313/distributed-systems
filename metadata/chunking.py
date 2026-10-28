import random

M64 = (1 << 64) - 1
_rng = random.Random(2026)
GEAR = [_rng.getrandbits(64) for _ in range(256)]


def fixed_chunks(data, size):
    return [data[i:i + size] for i in range(0, len(data), size)]


def _masks(avg_size):
    bits = avg_size.bit_length() - 1
    return ((1 << (bits + 1)) - 1) << (63 - bits), ((1 << (bits - 1)) - 1) << (65 - bits)


def _cut_point(data, start, end, min_size, avg_size, mask_s, mask_l):
    if end - start <= min_size:
        return end
    h = 0
    i = start + min_size
    normal = min(start + avg_size, end)
    while i < normal:
        h = ((h << 1) + GEAR[data[i]]) & M64
        if not h & mask_s:
            return i + 1
        i += 1
    while i < end:
        h = ((h << 1) + GEAR[data[i]]) & M64
        if not h & mask_l:
            return i + 1
        i += 1
    return end


def cdc_chunks(data, min_size, avg_size, max_size):
    mask_s, mask_l = _masks(avg_size)
    chunks = []
    start = 0
    while start < len(data):
        end = min(start + max_size, len(data))
        cut = _cut_point(data, start, end, min_size, avg_size, mask_s, mask_l)
        chunks.append(data[start:cut])
        start = cut
    return chunks


class StreamSplitter:
    def __init__(self, method, fixed_size, min_size, avg_size, max_size):
        self.method = method
        self.fixed_size = fixed_size
        self.min_size = min_size
        self.avg_size = avg_size
        self.max_size = max_size
        self.mask_s, self.mask_l = _masks(avg_size)
        self.buffer = bytearray()

    def feed(self, data):
        self.buffer += data
        pieces = []
        limit = self.fixed_size if self.method == "fixed" else self.max_size
        while len(self.buffer) >= limit:
            if self.method == "fixed":
                cut = self.fixed_size
            else:
                cut = _cut_point(
                    self.buffer, 0, self.max_size, self.min_size, self.avg_size, self.mask_s, self.mask_l
                )
            pieces.append(bytes(self.buffer[:cut]))
            del self.buffer[:cut]
        return pieces

    def finish(self):
        data = bytes(self.buffer)
        self.buffer = bytearray()
        if not data:
            return []
        if self.method == "fixed":
            return fixed_chunks(data, self.fixed_size)
        return cdc_chunks(data, self.min_size, self.avg_size, self.max_size)