import zfec


def encode(data, k, m):
    size = max(1, -(-len(data) // k))
    padded = data.ljust(size * k, b"\0")
    blocks = [padded[i * size:(i + 1) * size] for i in range(k)]
    return [bytes(b) for b in zfec.Encoder(k, k + m).encode(blocks)]


def decode(shares, k, m, length):
    numbers = sorted(shares)[:k]
    blocks = zfec.Decoder(k, k + m).decode([shares[n] for n in numbers], numbers)
    return b"".join(bytes(b) for b in blocks)[:length]