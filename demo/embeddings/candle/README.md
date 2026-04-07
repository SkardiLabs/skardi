# Candle Embedding Demo

This demo shows how to use the `candle()` scalar UDF to run local BERT-style
embedding inference directly inside SQL, and combine it with `lance_knn()` for
semantic search — no external embedding API, no Python in the hot path.

## How It Works

```sql
-- Embed the query on the fly, find the 5 nearest docs
SELECT id, title, content, _distance
FROM lance_knn(
  'doc_embeddings',
  'embedding',
  candle('models/bge-small-en-v1.5', {query}),
  10
)
ORDER BY _distance
LIMIT 10
```

`candle()` signature:

```sql
candle(model_dir, text_col [, normalize]) -> List<Float32>
```

| Argument | Description |
|---|---|
| `model_dir` | Path to a directory containing a `.safetensors` weights file, `config.json`, and `tokenizer.json`. |
| `text_col` | Text column or scalar to embed. |
| `normalize` | Optional boolean (default `true`). `true` → L2 unit-norm vectors for cosine similarity. `false` → raw mean-pooled vectors for dot-product search. |

The model is loaded and cached on first call — subsequent queries pay no loading cost.

### Supported Architectures

The architecture is detected automatically from `config.json`:

| `architectures` value | Model family |
|---|---|
| `BertModel`, `RobertaModel`, `XLMRobertaModel` | bge-\*, all-MiniLM-\*, e5-\*, … |
| `DistilBertModel` | distilbert-\* |
| `JinaBertModel` | jina-embeddings-\* |

## Prerequisites

1. **Python 3.12** and dependencies for the setup script:
   ```bash
   pip install fastembed lance huggingface_hub pyarrow
   ```
   > Python 3.12 is required — `onnxruntime` (used by `fastembed`) has no
   > pre-built wheels for Python 3.13+.

2. **Build the server** with the `candle` feature:
   ```bash
   cargo build --release -p skardi-server --features candle
   ```

## Setup

Run once from the **project root** to download the model and create the Lance dataset:

```bash
python demo/embeddings/candle/setup.py
```

This will:
- Download `BAAI/bge-small-en-v1.5` SafeTensors weights into `models/bge-small-en-v1.5/`
- Embed the 15 knowledge-base documents in `data/docs.csv`
- Write a Lance dataset to `demo/embeddings/candle/data/doc_embeddings.lance`

Expected output:
```
[1/3] Downloading BAAI/bge-small-en-v1.5 into models/bge-small-en-v1.5 ...
      model.safetensors: 133.4 MB
      config.json: 0.0 MB
      tokenizer.json: 0.7 MB
[2/3] Loaded 15 documents from demo/embeddings/candle/data/docs.csv
[3/3] Embedding 15 documents with BAAI/bge-small-en-v1.5 ...
      Embedding dimension: 384
      Lance dataset written to demo/embeddings/candle/data/doc_embeddings.lance
```

## Starting the Server

```bash
cargo run --bin skardi-server --features candle -- \
  --ctx demo/embeddings/candle/ctx.yaml \
  --pipeline demo/embeddings/candle/pipelines/ \
  --port 8080
```

## Running Queries

### Semantic Search

```bash
curl -X POST http://localhost:8080/semantic-search/execute \
  -H "Content-Type: application/json" \
  -d '{
    "query": "how does similarity search work in vector databases?"
  }'
```

**Response:**
```json
{
  "success": true,
  "data": [
    {
      "id": 1,
      "title": "Vector Databases",
      "content": "Vector databases store high-dimensional numerical vectors ...",
      "_distance": 0.082
    },
    {
      "id": 11,
      "title": "Approximate Nearest Neighbour Search",
      "content": "Exact nearest-neighbour search scales as O(n) per query ...",
      "_distance": 0.143
    },
    {
      "id": 9,
      "title": "Semantic Search",
      "content": "Semantic search retrieves documents based on meaning ...",
      "_distance": 0.178
    }
  ],
  "rows": 3,
  "execution_time_ms": 28
}
```

### More Example Queries

```bash
# Retrieval-Augmented Generation
curl -X POST http://localhost:8080/semantic-search/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "how to ground LLM responses with retrieved documents"}'

# Arrow / columnar formats
curl -X POST http://localhost:8080/semantic-search/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "columnar data formats for analytics"}'

# Model quantization
curl -X POST http://localhost:8080/semantic-search/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "running models on CPU without a GPU"}'
```

## Pipeline Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `query` | string | Yes | Free-text search query |

## Directory Layout

```
demo/embeddings/candle/
├── README.md
├── ctx.yaml                          — registers the Lance data source
├── setup.py                          — one-time setup: downloads model + creates Lance dataset
├── data/
│   ├── docs.csv                      — 15 knowledge-base documents (source of truth)
│   └── doc_embeddings.lance/         — created by setup.py
└── pipelines/
    └── pipeline_semantic_search.yaml — the semantic search pipeline
```

```
models/
└── bge-small-en-v1.5/               — created by setup.py
    ├── model.safetensors
    ├── config.json
    └── tokenizer.json
```

> **Note**: `models/` lives at the project root so the path in SQL
> (`models/bge-small-en-v1.5`) is relative to wherever you launch
> `skardi-server` from.

## Switching Models

Any HuggingFace embedding model in SafeTensors format works. Download a
different model and update the path in the pipeline SQL:

```bash
# Download a larger, higher-quality model
huggingface-cli download BAAI/bge-base-en-v1.5 \
  --include "model.safetensors" "config.json" "tokenizer.json" \
  --local-dir models/bge-base-en-v1.5
```

```sql
-- Use the larger model in the pipeline
candle('models/bge-base-en-v1.5', {query})
```

Re-run `setup.py` with `MODEL_ID = "BAAI/bge-base-en-v1.5"` and
`MODEL_DIR = Path("models/bge-base-en-v1.5")` to rebuild the Lance dataset
with the new model's embeddings.

## Troubleshooting

### "Failed to load candle model"
Ensure the path is relative to the directory where you started `skardi-server`
and that the model directory contains all three files: `model.safetensors`, `config.json`, `tokenizer.json`.

### "table 'doc_embeddings' not found"
Run `setup.py` first to create the Lance dataset.

### "Unknown architecture '...'; falling back to BertModel"
The model's `config.json` lists an architecture Skardi hasn't seen before.
It falls back to BERT, which covers most encoder models. If inference fails,
open an issue with the model name.

### Slow first query
The first call loads and caches the model (~133 MB for bge-small). Subsequent
queries are fast. Use `RUST_LOG=info` to see load timing in the server logs.
