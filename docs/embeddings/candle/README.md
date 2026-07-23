# Candle Embeddings

This guide shows how to use the `candle()` scalar UDF to run local BERT-style
embedding inference directly inside SQL, and combine it with `lance_knn()` for
semantic search — no external embedding API, no Python in the hot path.

## How It Works

```sql
-- Embed the query on the fly, find the 5 nearest docs
SELECT id, title, content, _distance
FROM lance_knn(
  'doc_embeddings',
  'embedding',
  candle('models/generated/bge-small-en-v1.5', {query}),
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
   pip install fastembed lance lancedb huggingface_hub pyarrow
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
python docs/embeddings/candle/setup.py
```

This will:
- Download `BAAI/bge-small-en-v1.5` SafeTensors weights into `models/generated/bge-small-en-v1.5/`
- Embed the 15 knowledge-base documents in `docs/embeddings/data/docs.csv`
- Write a Lance dataset to `docs/embeddings/candle/data/generated/doc_embeddings.lance`

Expected output:
```
[1/3] Downloading BAAI/bge-small-en-v1.5 into models/generated/bge-small-en-v1.5 ...
      model.safetensors: 133.4 MB
      config.json: 0.0 MB
      tokenizer.json: 0.7 MB
[2/3] Loaded 15 documents from docs/embeddings/data/docs.csv
[3/3] Embedding 15 documents with BAAI/bge-small-en-v1.5 ...
      Embedding dimension: 384
      Lance dataset written to docs/embeddings/candle/data/generated/doc_embeddings.lance
```

## Starting the Server

```bash
cargo run -p skardi-server --features candle -- \
  --ctx docs/embeddings/candle/ctx.yaml \
  --pipeline docs/embeddings/candle/pipelines/ \
  --port 8080
```

## Running Queries

The `semantic-search` pipeline loaded above is now reachable either directly
over HTTP or through the `skardi` CLI (a thin HTTP client — see
[docs/cli.md](../../cli.md)). Both forms send the same request.

### Semantic Search

```bash
curl -X POST http://localhost:8080/semantic-search/execute \
  -H "Content-Type: application/json" \
  -d '{
    "query": "how does similarity search work in vector databases?",
    "k": 10
  }' | jq .
```

Equivalent call via the CLI:

```bash
skardi run semantic-search \
  -p query="how does similarity search work in vector databases?" \
  -p k=10
```

**Response** (truncated — returns up to `k` results):
```json
{
      "id": 1,
      "title": "Vector Databases",
      "content": "Vector databases store high-dimensional numerical vectors and enable fast similarity search at scale. Unlike traditional databases that match exact values",
      "_distance": 0.3374274
    },
    {
      "id": 9,
      "title": "Semantic Search",
      "content": "Semantic search retrieves documents based on meaning rather than keyword overlap. A query and all documents are embedded into the same vector space; the most semantically similar documents are returned via nearest-neighbour search. This handles synonyms and paraphrases that would confuse keyword search.",
      "_distance": 0.5616751
    },
    {
      "id": 11,
      "title": "Approximate Nearest Neighbour Search",
      "content": "Exact nearest-neighbour search scales as O(n) per query. ANN algorithms like HNSW and IVF-PQ trade a small accuracy loss for sub-linear query times",
      "_distance": 0.5843972
    },
    {
      "id": 5,
      "title": "Cosine Similarity",
      "content": "Cosine similarity measures the angle between two vectors",
      "_distance": 0.6529644
    },
    {
      "id": 2,
      "title": "Retrieval-Augmented Generation",
      "content": "Retrieval-Augmented Generation (RAG) combines a retrieval step with a language model. A query is embedded into a vector and used to fetch relevant documents from a vector store. Those documents are passed as context to an LLM",
      "_distance": 0.7315137
    },
    {
      "id": 4,
      "title": "BERT Embeddings",
      "content": "BERT (Bidirectional Encoder Representations from Transformers) produces contextual embeddings by reading text in both directions. Fine-tuned variants like bge-small and all-MiniLM are commonly used for semantic similarity tasks. The [CLS] token or mean-pooled hidden states are used as sentence-level embeddings.",
      "_distance": 0.7570224
    },
    {
      "id": 10,
      "title": "SQL in ML Pipelines",
      "content": "Expressing ML pipelines in SQL keeps logic declarative and auditable. Features like CTEs",
      "_distance": 0.81387216
    },
    {
      "id": 8,
      "title": "Lance Format",
      "content": "Lance is a columnar storage format optimised for machine learning workloads. It supports random-access reads",
      "_distance": 0.83116364
    },
    {
      "id": 7,
      "title": "DataFusion Query Engine",
      "content": "DataFusion is an in-process SQL query engine written in Rust",
      "_distance": 0.8427545
    },
    {
      "id": 6,
      "title": "Apache Arrow",
      "content": "Apache Arrow defines a language-independent columnar memory format for flat and hierarchical data. It enables zero-copy reads between systems and is the in-memory format used by DataFusion",
      "_distance": 0.881372
    }
```

### More Example Queries

```bash
# Retrieval-Augmented Generation
curl -X POST http://localhost:8080/semantic-search/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "how to ground LLM responses with retrieved documents", "k": 10}' | jq .

# Arrow / columnar formats
curl -X POST http://localhost:8080/semantic-search/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "columnar data formats for analytics", "k": 5}' | jq .

# Model quantization
curl -X POST http://localhost:8080/semantic-search/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "running models on CPU without a GPU", "k": 5}' | jq .
```

## Pipeline Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `query` | string | Yes | Free-text search query |
| `k` | integer | Yes | Number of nearest neighbours to return |

## Directory Layout

```
docs/embeddings/candle/
├── README.md
├── ctx.yaml                          — registers the Lance data source
├── setup.py                          — one-time setup: downloads model + creates Lance dataset
├── data/
│   └── generated/
│       └── doc_embeddings.lance/     — created by setup.py
└── pipelines/
    └── pipeline_semantic_search.yaml — the semantic search pipeline
```

```
models/
└── generated/
    └── bge-small-en-v1.5/           — created by setup.py
    ├── model.safetensors
    ├── config.json
    └── tokenizer.json
```

> **Note**: `models/` lives at the project root so the path in SQL
> (`models/generated/bge-small-en-v1.5`) is relative to wherever you launch
> `skardi-server` from.

## Switching Models

Any HuggingFace embedding model in SafeTensors format works. Download a
different model and update the path in the pipeline SQL:

```bash
# Download a larger, higher-quality model
huggingface-cli download BAAI/bge-base-en-v1.5 \
  --include "model.safetensors" "config.json" "tokenizer.json" \
  --local-dir models/generated/bge-base-en-v1.5
```

```sql
-- Use the larger model in the pipeline
candle('models/generated/bge-base-en-v1.5', {query})
```

Re-run `setup.py` with `MODEL_ID = "BAAI/bge-base-en-v1.5"` and
`MODEL_DIR = Path("models/generated/bge-base-en-v1.5")` to rebuild the Lance dataset
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
