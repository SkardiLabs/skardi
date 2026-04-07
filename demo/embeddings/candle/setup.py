"""
Setup script for the candle embedding demo.

What this does:
  1. Downloads bge-small-en-v1.5 SafeTensors weights into models/ so Skardi
     can load them via candle().
  2. Embeds docs.csv using the same model (via fastembed / ONNX Runtime) and
     writes the result as a Lance dataset at data/doc_embeddings.lance.

Requirements:
  Python 3.12 is required (onnxruntime has no wheels for 3.13+).

Usage (run from the project root):
  pip install fastembed lancedb huggingface_hub pyarrow
  python demo/embeddings/candle/setup.py
"""

import os
import sys
import csv
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths (all relative to the project root, where you run this script from)
# ---------------------------------------------------------------------------

DEMO_DIR = Path("demo/embeddings/candle")
DOCS_CSV = DEMO_DIR / "data" / "docs.csv"
LANCE_OUT = DEMO_DIR / "data" / "doc_embeddings.lance"
MODEL_DIR = Path("models/bge-small-en-v1.5")
MODEL_ID = "BAAI/bge-small-en-v1.5"

# ---------------------------------------------------------------------------
# Step 1: Download model weights for Skardi (SafeTensors only)
# ---------------------------------------------------------------------------


def download_model():
    required = ["model.safetensors", "config.json", "tokenizer.json"]
    if all((MODEL_DIR / f).exists() for f in required):
        print(f"[1/3] Model already present at {MODEL_DIR}/, skipping download.")
        return

    print(f"[1/3] Downloading {MODEL_ID} into {MODEL_DIR} ...")
    from huggingface_hub import snapshot_download

    snapshot_download(
        repo_id=MODEL_ID,
        allow_patterns=required,
        local_dir=str(MODEL_DIR),
    )
    print(f"      Model files saved to {MODEL_DIR}/")
    for f in required:
        path = MODEL_DIR / f
        size = path.stat().st_size / 1024 / 1024
        print(f"      {f}: {size:.1f} MB")


# ---------------------------------------------------------------------------
# Step 2: Load docs
# ---------------------------------------------------------------------------


def load_docs():
    rows = []
    with open(DOCS_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    print(f"[2/3] Loaded {len(rows)} documents from {DOCS_CSV}")
    return rows


# ---------------------------------------------------------------------------
# Step 3: Embed and write Lance dataset
# ---------------------------------------------------------------------------


def create_lance_dataset(docs):
    print(f"[3/3] Embedding {len(docs)} documents with {MODEL_ID} ...")
    from fastembed import TextEmbedding
    import lancedb
    import pyarrow as pa

    # fastembed uses ONNX Runtime — no torch/numpy conflicts.
    # It normalises embeddings by default, matching candle(..., true).
    embed_model = TextEmbedding(MODEL_ID)

    contents = [d["content"] for d in docs]
    embeddings = list(embed_model.embed(contents))

    dim = len(embeddings[0])
    print(f"      Embedding dimension: {dim}")

    table = pa.table(
        {
            "id": pa.array([int(d["id"]) for d in docs], type=pa.int64()),
            "title": pa.array([d["title"] for d in docs], type=pa.utf8()),
            "content": pa.array([d["content"] for d in docs], type=pa.utf8()),
            "embedding": pa.array(
                [emb.tolist() for emb in embeddings],
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
    # Verify we are running from the project root
    if not Path("Cargo.toml").exists():
        print("ERROR: Run this script from the Skardi project root.")
        print("  cd /path/to/skardi && python demo/embeddings/candle/setup.py")
        sys.exit(1)

    (DEMO_DIR / "data").mkdir(parents=True, exist_ok=True)
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    download_model()
    docs = load_docs()
    create_lance_dataset(docs)

    print()
    print("Setup complete. Start the demo server with:")
    print()
    print("  cargo run --bin skardi-server --features candle -- \\")
    print(f"    --ctx {DEMO_DIR}/ctx.yaml \\")
    print(f"    --pipeline {DEMO_DIR}/pipelines/ \\")
    print("    --port 8080")


if __name__ == "__main__":
    main()
