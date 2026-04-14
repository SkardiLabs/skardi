#!/usr/bin/env python3
"""Create the SQLite database for the RAG CLI demo.

Builds the schema only — content and embeddings are ingested later through
`skardi query` (which uses the `candle()` UDF for inline embedding).

Schema:
    documents       canonical content + packed-f32 embedding BLOB
    documents_fts   FTS5 mirror (id UNINDEXED so PRAGMA introspection sees it)
    documents_vec   sqlite-vec vec0 mirror, float[384] embeddings
    documents_ai    AFTER INSERT trigger fanning rows out to both mirrors

Usage:
    pip install sqlite-vec
    python demo/rag/setup.py
"""

import os
import sqlite3

import sqlite_vec

DB_PATH = "demo/rag/rag.db"

SCHEMA = """
CREATE TABLE documents (
    id        INTEGER PRIMARY KEY,
    content   TEXT NOT NULL,
    embedding BLOB NOT NULL
);

CREATE VIRTUAL TABLE documents_fts USING fts5(id UNINDEXED, content);

CREATE VIRTUAL TABLE documents_vec USING vec0(
    id        INTEGER PRIMARY KEY,
    embedding float[384]
);

CREATE TRIGGER documents_ai AFTER INSERT ON documents BEGIN
    INSERT INTO documents_fts(id, content) VALUES (NEW.id, NEW.content);
    INSERT INTO documents_vec(id, embedding) VALUES (NEW.id, NEW.embedding);
END;
"""


def main():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    db = sqlite3.connect(DB_PATH)
    db.enable_load_extension(True)
    sqlite_vec.load(db)
    db.enable_load_extension(False)

    db.executescript(SCHEMA)
    db.commit()
    db.close()

    print(f"Created {DB_PATH}")
    print("  documents, documents_fts, documents_vec ready for ingest via skardi query")


if __name__ == "__main__":
    main()
