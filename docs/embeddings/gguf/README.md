# GGUF Embeddings

This guide shows how to use the `gguf()` scalar UDF to run local GGUF-format
embedding inference directly inside SQL via llama.cpp, and combine it with
`lance_knn()` for semantic search — no external embedding API, no Python in the
hot path.

## How It Works

```sql
-- Embed the query on the fly, find the 10 nearest docs
SELECT id, title, content, _distance
FROM lance_knn(
  'doc_embeddings_gguf',
  'embedding',
  gguf('models/generated/embeddinggemma-300m', {query}),
  10
)
ORDER BY _distance
LIMIT 10
```

`gguf()` signature:

```sql
gguf(model_dir, text_col [, normalize]) -> List<Float32>
```

| Argument | Description |
|---|---|
| `model_dir` | Path to a directory containing a single `.gguf` weights file. |
| `text_col` | Text column or scalar to embed. |
| `normalize` | Optional boolean (default `true`). `true` → L2 unit-norm vectors for cosine similarity. `false` → raw vectors for dot-product search. |

The model is loaded and cached on first call — subsequent queries pay no loading cost.

### Why GGUF?

GGUF models use the llama.cpp inference backend, which natively handles tensor
naming, tokenization, and architecture wiring for all supported model families.
No manual `config.json` or architecture detection needed — everything is embedded
in the `.gguf` file.

GGUF also supports multiple quantisation levels (Q4, Q5, Q8, F16, F32), making
it possible to run embedding models with a fraction of the memory footprint.

## Prerequisites

1. **Python 3.12** and dependencies for the setup script:
   ```bash
   pip install llama-cpp-python lancedb huggingface_hub pyarrow
   ```

2. **Accept Google's Gemma licence** (required for the tokenizer):
   - Visit https://huggingface.co/google/embeddinggemma-300m-qat-q8_0-unquantized
   - Accept the licence agreement
   - Authenticate: `huggingface-cli login`

3. **Build the server** with the `gguf` feature:
   ```bash
   cargo build --release -p skardi-server --features gguf
   ```

## Setup

Run once from the **project root** to download the model and create the Lance dataset:

```bash
python docs/embeddings/gguf/setup_gguf.py
```

This will:
- Download `embeddinggemma-300m-qat-Q8_0.gguf` (~329 MB) into `models/generated/embeddinggemma-300m/`
- Download `tokenizer.json` from the gated Gemma repo
- Embed the 15 knowledge-base documents in `docs/embeddings/data/docs.csv`
- Write a Lance dataset to `docs/embeddings/gguf/data/generated/doc_embeddings_gguf.lance`

Expected output:
```
[1/4] Downloading embeddinggemma-300m-qat-Q8_0.gguf from ggml-org/embeddinggemma-300m-qat-q8_0-GGUF (~329 MB) ...
      embeddinggemma-300m-qat-Q8_0.gguf: 329.0 MB
[2/4] Downloading tokenizer.json from google/embeddinggemma-300m-qat-q8_0-unquantized ...
      tokenizer.json: 31.8 MB
[3/4] Loaded 15 documents from docs/embeddings/data/docs.csv
[4/4] Embedding 15 documents with embeddinggemma-300m-qat-Q8_0.gguf ...
      Embedding dimension: 256
      Lance dataset written to docs/embeddings/gguf/data/generated/doc_embeddings_gguf.lance
```

## Starting the Server

```bash
cargo run -p skardi-server --features gguf -- \
  --ctx docs/embeddings/gguf/ctx.yaml \
  --pipeline docs/embeddings/gguf/pipelines/ \
  --port 8080
```

## Running Queries

The `semantic-search-gguf` pipeline loaded above is now reachable either
directly over HTTP or through the `skardi` CLI (a thin HTTP client — see
[docs/cli.md](../../cli.md)). Both forms send the same request.

### Semantic Search

```bash
curl -X POST http://localhost:8080/semantic-search-gguf/execute \
  -H "Content-Type: application/json" \
  -d '{
    "query": "how does similarity search work in vector databases?",
    "k": 10
  }' | jq .
```

Equivalent call via the CLI:

```bash
skardi run semantic-search-gguf \
  -p query="how does similarity search work in vector databases?" \
  -p k=10
```

**Response**:
```json
{
  "success": true,
  "data": [
    {
      "id": 9,
      "title": "Semantic Search",
      "content": "Semantic search retrieves documents based on meaning rather than keyword overlap. A query and all documents are embedded into the same vector space; the most semantically similar documents are returned via nearest-neighbour search. This handles synonyms and paraphrases that would confuse keyword search.",
      "_distance": 189.5809
    },
    {
      "id": 4,
      "title": "BERT Embeddings",
      "content": "BERT (Bidirectional Encoder Representations from Transformers) produces contextual embeddings by reading text in both directions. Fine-tuned variants like bge-small and all-MiniLM are commonly used for semantic similarity tasks. The [CLS] token or mean-pooled hidden states are used as sentence-level embeddings.",
      "_distance": 197.44229
    },
    {
      "id": 2,
      "title": "Retrieval-Augmented Generation",
      "content": "Retrieval-Augmented Generation (RAG) combines a retrieval step with a language model. A query is embedded into a vector and used to fetch relevant documents from a vector store. Those documents are passed as context to an LLM",
      "_distance": 216.03638
    },
    {
      "id": 11,
      "title": "Approximate Nearest Neighbour Search",
      "content": "Exact nearest-neighbour search scales as O(n) per query. ANN algorithms like HNSW and IVF-PQ trade a small accuracy loss for sub-linear query times",
      "_distance": 221.05103
    },
    {
      "id": 13,
      "title": "Mean Pooling",
      "content": "Mean pooling aggregates the per-token hidden states from a transformer encoder into a single fixed-size vector. Each token embedding is averaged across the sequence length",
      "_distance": 226.3469
    },
    {
      "id": 6,
      "title": "Apache Arrow",
      "content": "Apache Arrow defines a language-independent columnar memory format for flat and hierarchical data. It enables zero-copy reads between systems and is the in-memory format used by DataFusion",
      "_distance": 236.95584
    },
    {
      "id": 12,
      "title": "GGUF Format",
      "content": "GGUF (GGML Universal Format) is a binary format for storing quantised neural network weights. Developed by the llama.cpp project",
      "_distance": 264.68677
    },
    {
      "id": 1,
      "title": "Vector Databases",
      "content": "Vector databases store high-dimensional numerical vectors and enable fast similarity search at scale. Unlike traditional databases that match exact values",
      "_distance": 272.3685
    },
    {
      "id": 8,
      "title": "Lance Format",
      "content": "Lance is a columnar storage format optimised for machine learning workloads. It supports random-access reads",
      "_distance": 315.6844
    },
    {
      "id": 3,
      "title": "Transformer Architecture",
      "content": "The Transformer architecture introduced multi-head self-attention to replace recurrent networks. Each token attends to all other tokens in the sequence",
      "_distance": 330.20328
    }
  ],
  "rows": 10,
  "execution_time_ms": 890,
  "timestamp": "2026-04-08T16:53:11.328699+00:00"
}
```

### More Example Queries

```bash
# Retrieval-Augmented Generation
curl -X POST http://localhost:8080/semantic-search-gguf/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "how to ground LLM responses with retrieved documents", "k": 10}' | jq .

# GGUF and quantisation
curl -X POST http://localhost:8080/semantic-search-gguf/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "running quantised models on CPU without a GPU", "k": 5}' | jq .

# Arrow / columnar formats
curl -X POST http://localhost:8080/semantic-search-gguf/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "columnar data formats for analytics", "k": 5}' | jq .
```

## Pipeline Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `query` | string | Yes | Free-text search query |
| `k` | integer | Yes | Number of nearest neighbours to return |

## Directory Layout

```
docs/embeddings/gguf/
├── README.md
├── ctx.yaml                          — registers the Lance data source
├── setup_gguf.py                     — one-time setup: downloads model + creates Lance dataset
├── data/
│   └── generated/
│       └── doc_embeddings_gguf.lance/ — created by setup_gguf.py
└── pipelines/
    └── pipeline_semantic_search_gguf.yaml — the semantic search pipeline
```

```
models/
└── generated/
    └── embeddinggemma-300m/          — created by setup_gguf.py
    ├── embeddinggemma-300m-qat-Q8_0.gguf
    └── tokenizer.json
```

> **Note**: `models/` lives at the project root so the path in SQL
> (`models/generated/embeddinggemma-300m`) is relative to wherever you launch
> `skardi-server` from.

## Switching Models

Any GGUF embedding model works. Download a different model and update the path
in the pipeline SQL:

```sql
-- Use a different GGUF model in the pipeline
gguf('models/generated/nomic-embed-text-v1.5', {query})
```

Re-run `setup_gguf.py` with updated `MODEL_DIR` / `GGUF_REPO` / `GGUF_FILE`
to rebuild the Lance dataset with the new model's embeddings.

## Troubleshooting

### "Failed to load GGUF model"
Ensure the path is relative to the directory where you started `skardi-server`
and that the model directory contains a single `.gguf` file.

### "table 'doc_embeddings_gguf' not found"
Run `setup_gguf.py` first to create the Lance dataset.

### "Could not download tokenizer.json"
The tokenizer is hosted in a gated repo. Accept the Gemma licence at
https://huggingface.co/google/embeddinggemma-300m-qat-q8_0-unquantized
and run `huggingface-cli login`.

### Slow first query
The first call loads and caches the model (~329 MB). Subsequent queries are fast.
Use `RUST_LOG=info` to see load timing in the server logs.
