#!/usr/bin/env python3
"""Create the SQLite KNN demo database with sqlite-vec vec0 table.

Embeds item names using fastembed (BAAI/bge-small-en-v1.5, 384 dims)
and inserts them into the vec0 virtual table.

Usage:
    pip install sqlite-vec fastembed
    python demo/sqlite/setup_knn_demo.py
"""

import os
import sqlite3
import struct

import sqlite_vec

DB_PATH = "demo/sqlite/knn_demo.db"

ITEMS = [
    (1, "Laptop", "electronics"),
    (2, "Book", "education"),
    (3, "Headphones", "electronics"),
    (4, "Tablet", "electronics"),
    (5, "Notebook", "education"),
]


def embed_texts(texts: list[str]) -> list[list[float]]:
    """Embed texts using fastembed (ONNX Runtime, no PyTorch needed)."""
    try:
        from fastembed import TextEmbedding

        model = TextEmbedding("BAAI/bge-small-en-v1.5")  # 384 dims
        return [vec.tolist() for vec in model.embed(texts)]
    except Exception as e:
        print(f"fastembed unavailable ({e}), using random vectors for demo")
        import random

        random.seed(42)
        return [[random.gauss(0, 1) for _ in range(384)] for _ in texts]


def pack_f32(vec: list[float]) -> bytes:
    """Pack a list of floats into little-endian f32 bytes."""
    return struct.pack(f"<{len(vec)}f", *vec)


def main():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    db = sqlite3.connect(DB_PATH)
    db.enable_load_extension(True)
    sqlite_vec.load(db)
    db.enable_load_extension(False)

    db.executescript("""
        CREATE TABLE items (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL
        );

        CREATE VIRTUAL TABLE vec_items USING vec0(
            item_id INTEGER PRIMARY KEY,
            embedding float[384]
        );
    """)

    # Insert item metadata
    db.executemany(
        "INSERT INTO items (id, name, category) VALUES (?, ?, ?)",
        ITEMS,
    )

    # Embed item names and insert vectors
    texts = [name for _, name, _ in ITEMS]
    embeddings = embed_texts(texts)

    for (item_id, _, _), vec in zip(ITEMS, embeddings):
        db.execute(
            "INSERT INTO vec_items (item_id, embedding) VALUES (?, ?)",
            (item_id, pack_f32(vec)),
        )

    db.commit()

    # Verify
    n_items = db.execute("SELECT count(*) FROM items").fetchone()[0]
    n_vecs = db.execute("SELECT count(*) FROM vec_items").fetchone()[0]
    db.close()

    print(f"Created {DB_PATH}")
    print(f"  items:     {n_items} rows")
    print(f"  vec_items: {n_vecs} vectors (384 dims)")


if __name__ == "__main__":
    main()
