import os
import time

import psycopg

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://storage:storage@postgres:5432/storage")

SCHEMA = """
CREATE TABLE IF NOT EXISTS buckets (
    name TEXT PRIMARY KEY,
    redundancy TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS objects (
    id SERIAL PRIMARY KEY,
    bucket TEXT NOT NULL REFERENCES buckets(name),
    key TEXT NOT NULL,
    version INTEGER NOT NULL,
    size BIGINT NOT NULL,
    chunking TEXT NOT NULL,
    redundancy TEXT NOT NULL,
    chunk_hashes TEXT[] NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (bucket, key, version)
);
CREATE TABLE IF NOT EXISTS chunks (
    hash TEXT NOT NULL,
    scheme TEXT NOT NULL,
    size INTEGER NOT NULL,
    ref_count INTEGER NOT NULL,
    fragments JSONB NOT NULL,
    PRIMARY KEY (hash, scheme)
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