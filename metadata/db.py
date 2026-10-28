import os
import time

import psycopg

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://storage:storage@postgres:5432/storage")

SCHEMA = """
CREATE TABLE IF NOT EXISTS objects (
    id SERIAL PRIMARY KEY,
    bucket TEXT NOT NULL,
    key TEXT NOT NULL,
    version INTEGER NOT NULL,
    size BIGINT NOT NULL,
    chunking TEXT NOT NULL,
    chunk_hashes TEXT[] NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (bucket, key, version)
);
CREATE TABLE IF NOT EXISTS chunks (
    hash TEXT PRIMARY KEY,
    size INTEGER NOT NULL,
    ref_count INTEGER NOT NULL,
    nodes TEXT[] NOT NULL
);
"""


def connect():
    return psycopg.connect(DATABASE_URL)


def init_db():
    for _ in range(30):
        try:
            with connect() as conn:
                conn.execute(SCHEMA)
            return
        except psycopg.OperationalError:
            time.sleep(1)
    raise RuntimeError("Could not connect to PostgreSQL")