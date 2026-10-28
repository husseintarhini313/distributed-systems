import random

M64 = (1 << 64) - 1
_rng = random.Random(2026)
GEAR = [_rng.getrandbits(64) for _ in range(256)]


def fixed_chunks(data, size):
    return [data[i:i + size] for i in range(0, len(data), size)]


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
    bits = avg_size.bit_length() - 1
    mask_s = ((1 << (bits + 1)) - 1) << (63 - bits)
    mask_l = ((1 << (bits - 1)) - 1) << (65 - bits)
    chunks = []
    start = 0
    while start < len(data):
        end = min(start + max_size, len(data))
        cut = _cut_point(data, start, end, min_size, avg_size, mask_s, mask_l)
        chunks.append(data[start:cut])
        start = cut
    return chunks