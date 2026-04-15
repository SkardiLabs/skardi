"""
Setup script for the remote embedding demo (OpenAI).

Uses OpenAI's text-embedding-3-small to embed a small knowledge base and
write a Lance dataset for semantic search via Skardi's remote_embed() UDF.

Prerequisites:
  1. Set your OpenAI API key:
       export OPENAI_API_KEY=sk-...
  2. Install Python dependencies:
       pip install openai lancedb pyarrow

Usage (run from the project root):
  python docs/embeddings/remote/setup_remote.py
"""

import csv
import os
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths (all relative to the project root)
# ---------------------------------------------------------------------------

DEMO_DIR = Path("docs/embeddings/remote")
DOCS_CSV = Path("docs/embeddings/data/docs.csv")
LANCE_OUT = DEMO_DIR / "data" / "generated" / "doc_embeddings_openai.lance"

MODEL = "text-embedding-3-small"
EMBEDDING_DIM = 1536


# ---------------------------------------------------------------------------
# Step 1: Load docs
# ---------------------------------------------------------------------------


def load_docs():
    rows = []
    with open(DOCS_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    print(f"[1/3] Loaded {len(rows)} documents from {DOCS_CSV}")
    return rows


# ---------------------------------------------------------------------------
# Step 2: Embed with OpenAI
# ---------------------------------------------------------------------------


def embed_docs(docs):
    print(f"[2/3] Embedding {len(docs)} documents with {MODEL} ...")
    from openai import OpenAI

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("ERROR: OPENAI_API_KEY environment variable is not set.")
        print("  export OPENAI_API_KEY=sk-...")
        sys.exit(1)

    client = OpenAI(api_key=api_key)
    contents = [d["content"] for d in docs]

    response = client.embeddings.create(input=contents, model=MODEL)
    embeddings = [item.embedding for item in response.data]

    dim = len(embeddings[0])
    print(f"      Embedding dimension: {dim}")
    return embeddings


# ---------------------------------------------------------------------------
# Step 3: Write Lance dataset
# ---------------------------------------------------------------------------


def create_lance_dataset(docs, embeddings):
    print(f"[3/3] Writing Lance dataset to {LANCE_OUT} ...")
    import lancedb
    import pyarrow as pa

    dim = len(embeddings[0])
    table = pa.table(
        {
            "id": pa.array([int(d["id"]) for d in docs], type=pa.int64()),
            "title": pa.array([d["title"] for d in docs], type=pa.utf8()),
            "content": pa.array([d["content"] for d in docs], type=pa.utf8()),
            "embedding": pa.array(
                embeddings,
                type=pa.list_(pa.float32(), dim),
            ),
        }
    )

    db = lancedb.connect(str(LANCE_OUT.parent))
    db.create_table(LANCE_OUT.stem, data=table, mode="overwrite")
    print(f"      Lance dataset written to {LANCE_OUT}")
    print(f"      Schema: {table.schema}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    if not Path("Cargo.toml").exists():
        print("ERROR: Run this script from the Skardi project root.")
        print("  cd /path/to/skardi && python docs/embeddings/remote/setup_remote.py")
        sys.exit(1)

    (DEMO_DIR / "data" / "generated").mkdir(parents=True, exist_ok=True)

    docs = load_docs()
    embeddings = embed_docs(docs)
    create_lance_dataset(docs, embeddings)

    print()
    print("Setup complete. Start the demo server with:")
    print()
    print("  cargo run --bin skardi-server --features remote-embed -- \\")
    print(f"    --ctx {DEMO_DIR}/ctx.yaml \\")
    print(f"    --pipeline {DEMO_DIR}/pipelines/ \\")
    print("    --port 8080")


if __name__ == "__main__":
    main()
