"""
Setup script for the GGUF embedding demo.

Uses EmbeddingGemma-300m (quantised to Q8_0 in GGUF format) for local
semantic search — no external API, no Python in the hot path.

What this does:
  1. Downloads embeddinggemma-300m-qat-Q8_0.gguf from ggml-org on HuggingFace.
  2. Downloads tokenizer.json from the base model (requires accepting Google's
     Gemma license — see Prerequisites).
  3. Embeds docs.csv using llama-cpp-python and writes a Lance dataset at
     data/generated/doc_embeddings_gguf.lance.

Prerequisites:
  1. Accept Google's EmbeddingGemma licence at
     https://huggingface.co/google/embeddinggemma-300m-qat-q8_0-unquantized
  2. Authenticate the HuggingFace CLI:
       huggingface-cli login
  3. Install Python dependencies:
       pip install llama-cpp-python lancedb huggingface_hub pyarrow

Usage (run from the project root):
  python docs/embeddings/gguf/setup_gguf.py
"""

import sys
import csv
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths (all relative to the project root, where you run this script from)
# ---------------------------------------------------------------------------

DEMO_DIR = Path("docs/embeddings/gguf")
DOCS_CSV = Path("docs/embeddings/data/docs.csv")
LANCE_OUT = DEMO_DIR / "data" / "generated" / "doc_embeddings_gguf.lance"
MODEL_DIR = Path("models/generated/embeddinggemma-300m")

GGUF_REPO = "ggml-org/embeddinggemma-300m-qat-q8_0-GGUF"
GGUF_FILE = "embeddinggemma-300m-qat-Q8_0.gguf"
TOKENIZER_REPO = "google/embeddinggemma-300m-qat-q8_0-unquantized"

GGUF_PATH = MODEL_DIR / GGUF_FILE
TOKENIZER_PATH = MODEL_DIR / "tokenizer.json"


# ---------------------------------------------------------------------------
# Step 1: Download the GGUF weights (public, no auth needed)
# ---------------------------------------------------------------------------


def download_gguf():
    if GGUF_PATH.exists():
        print(f"[1/4] GGUF already present at {GGUF_PATH}, skipping download.")
        return

    print(f"[1/4] Downloading {GGUF_FILE} from {GGUF_REPO} (~329 MB) ...")
    from huggingface_hub import hf_hub_download

    hf_hub_download(
        repo_id=GGUF_REPO,
        filename=GGUF_FILE,
        local_dir=str(MODEL_DIR),
    )
    size = GGUF_PATH.stat().st_size / 1024 / 1024
    print(f"      {GGUF_FILE}: {size:.1f} MB")


# ---------------------------------------------------------------------------
# Step 2: Download the tokenizer (gated — requires Gemma licence + HF login)
# ---------------------------------------------------------------------------


def download_tokenizer():
    if TOKENIZER_PATH.exists():
        print(f"[2/4] tokenizer.json already present, skipping download.")
        return

    print(f"[2/4] Downloading tokenizer.json from {TOKENIZER_REPO} ...")
    print(f"      (Requires: huggingface-cli login + Gemma licence accepted)")
    try:
        from huggingface_hub import hf_hub_download

        hf_hub_download(
            repo_id=TOKENIZER_REPO,
            filename="tokenizer.json",
            local_dir=str(MODEL_DIR),
        )
        size = TOKENIZER_PATH.stat().st_size / 1024 / 1024
        print(f"      tokenizer.json: {size:.1f} MB")
    except Exception as e:
        print()
        print("ERROR: Could not download tokenizer.json.")
        print(f"  {e}")
        print()
        print("  To fix this:")
        print(
            f"  1. Accept the Gemma licence at https://huggingface.co/{TOKENIZER_REPO}"
        )
        print("  2. Run:  huggingface-cli login")
        print("  3. Re-run this setup script.")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Step 3: Load docs
# ---------------------------------------------------------------------------


def load_docs():
    rows = []
    with open(DOCS_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    print(f"[3/4] Loaded {len(rows)} documents from {DOCS_CSV}")
    return rows


# ---------------------------------------------------------------------------
# Step 4: Embed with llama-cpp-python and write Lance dataset
# ---------------------------------------------------------------------------


def create_lance_dataset(docs):
    print(f"[4/4] Embedding {len(docs)} documents with {GGUF_FILE} ...")
    from llama_cpp import Llama
    import lancedb
    import pyarrow as pa

    llm = Llama(
        model_path=str(GGUF_PATH),
        embedding=True,
        n_ctx=2048,
        verbose=False,
    )

    contents = [d["content"] for d in docs]
    embeddings = [
        llm.create_embedding(text)["data"][0]["embedding"] for text in contents
    ]

    dim = len(embeddings[0])
    print(f"      Embedding dimension: {dim}")

    table = pa.table(
        {
            "id": pa.array([int(d["id"]) for d in docs], type=pa.int64()),
            "title": pa.array([d["title"] for d in docs], type=pa.utf8()),
            "content": pa.array([d["content"] for d in docs], type=pa.utf8()),
            "embedding": pa.array(
                [emb for emb in embeddings],
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
        print("  cd /path/to/skardi && python docs/embeddings/gguf/setup_gguf.py")
        sys.exit(1)

    (DEMO_DIR / "data" / "generated").mkdir(parents=True, exist_ok=True)
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    download_gguf()
    download_tokenizer()
    docs = load_docs()
    create_lance_dataset(docs)

    print()
    print("Setup complete. Start the demo server with:")
    print()
    print("  cargo run --bin skardi-server --features gguf -- \\")
    print(f"    --ctx {DEMO_DIR}/ctx.yaml \\")
    print(f"    --pipeline {DEMO_DIR}/pipelines/ \\")
    print("    --port 8080")


if __name__ == "__main__":
    main()
