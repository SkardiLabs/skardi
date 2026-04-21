# SeekDB Integration

[SeekDB](https://github.com/oceanbase/seekdb) is an AI-native search database
built on OceanBase. It speaks the MySQL wire protocol and adds native:

- **Full-text search** via `FULLTEXT` indexes with the IK parser.
- **KNN vector search** via `VECTOR` columns with HNSW indexing.

Skardi exposes all three — CRUD scan/insert/update/delete, `seekdb_fts`
for full-text ranking, and `seekdb_knn` for nearest-neighbour search — through
a single `type: "seekdb"` data source.

## Quick Start (Docker)

```bash
# 1. Start SeekDB in Docker. The image is multi-GB; the first pull takes a few
#    minutes. SeekDB exposes the MySQL-protocol port on 2881 and the RPC port
#    on 2886. Data persists under ./data.
docker run -d --name seekdb \
  -p 2881:2881 -p 2886:2886 \
  -v "$(pwd)/data:/var/lib/oceanbase" \
  oceanbase/seekdb:latest

# 2. Wait until the server is accepting MySQL connections.
until mysql -h 127.0.0.1 -P 2881 -u "root@sys" -e "SELECT 1" >/dev/null 2>&1; do
  sleep 3
done

# 3. Seed demo data.
mysql -h 127.0.0.1 -P 2881 -u "root@sys" <<'EOF'
CREATE DATABASE IF NOT EXISTS mydb;
USE mydb;

CREATE TABLE users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL
);
CREATE TABLE orders (
    id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT NOT NULL,
    product VARCHAR(100) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL
);

-- FULLTEXT + IK parser for multi-language tokenisation.
CREATE TABLE articles (
    id INT PRIMARY KEY AUTO_INCREMENT,
    title VARCHAR(200) NOT NULL,
    body TEXT NOT NULL,
    category VARCHAR(50) NOT NULL,
    FULLTEXT INDEX ft_body (body) WITH PARSER IK
);

-- VECTOR column + HNSW index.
CREATE TABLE docs (
    id INT PRIMARY KEY AUTO_INCREMENT,
    title VARCHAR(200) NOT NULL,
    category VARCHAR(50) NOT NULL,
    embedding VECTOR(4),
    VECTOR INDEX idx_embedding (embedding) WITH (TYPE = HNSW, DISTANCE = L2)
);

INSERT INTO users (name, email) VALUES
    ('Alice Smith', 'alice@example.com'),
    ('Bob Johnson', 'bob@example.com'),
    ('Carol Williams', 'carol@example.com');

INSERT INTO orders (user_id, product, amount) VALUES
    (1, 'Laptop',   999.99),
    (2, 'Keyboard',  79.99),
    (3, 'Monitor',  299.99);

INSERT INTO articles (title, body, category) VALUES
    ('Intro to Machine Learning',    'machine learning model training deep neural network supervised algorithms', 'ai'),
    ('Natural Language Processing',  'natural language processing text classification sentiment analysis',       'ai'),
    ('Database Query Optimization',  'database query optimization indexing performance tuning relational',       'database'),
    ('Deep Learning Advances',       'machine learning classification supervised training model convolutional',   'research'),
    ('Neural Network Architectures', 'deep learning neural network convolutional image recognition transformer',  'ai');

INSERT INTO docs (title, category, embedding) VALUES
    ('doc-a', 'electronics', '[1.0, 0.0, 0.0, 0.0]'),
    ('doc-b', 'electronics', '[0.0, 1.0, 0.0, 0.0]'),
    ('doc-c', 'books',       '[0.0, 0.0, 1.0, 0.0]'),
    ('doc-d', 'electronics', '[1.0, 1.0, 0.0, 0.0]'),
    ('doc-e', 'books',       '[0.5, 0.5, 0.5, 0.5]');
EOF

# 4. Set environment variables used by the demo ctx file.
export SEEKDB_USER="root@sys"
export SEEKDB_PASSWORD=""

# 5. Start Skardi against the bundled demo context + pipelines.
cargo run --bin skardi-server -- \
  --ctx docs/seekdb/ctx_seekdb_demo.yaml \
  --pipeline docs/seekdb/pipelines/ \
  --port 8080
```

## Prerequisites

1. Running SeekDB instance on port 2881 (`docker ps` shows the container).
2. A MySQL-protocol client (the standard `mysql` CLI or `obclient` both work).
3. `SEEKDB_USER` / `SEEKDB_PASSWORD` exported — SeekDB's default root user is
   `root@sys` with an empty password.

## Data Model

SeekDB presents as a MySQL-compatible relational database. In Skardi:

- One `type: "seekdb"` data source per table (or one in `hierarchy_level: catalog`
  mode for the whole schema).
- Columns map 1:1 to Arrow types via `information_schema.columns`. `VECTOR` is
  intentionally excluded from the visible schema — it's only surfaced through
  `seekdb_knn`.
- Access mode defaults to `read_only`. Set `access_mode: "read_write"` to
  allow `INSERT` / `UPDATE` / `DELETE`.

## Available Pipelines

| Pipeline file | What it demonstrates |
|---|---|
| `pipelines/query_user_by_id.yaml` | Point lookup on `users` |
| `pipelines/search_users_by_email.yaml` | `LIKE`-based filter |
| `pipelines/insert_user.yaml` | `INSERT` (read-write) |
| `pipelines/update_user_email.yaml` | `UPDATE` (read-write) |
| `pipelines/delete_user.yaml` | `DELETE` (read-write) |
| `pipelines/user_orders_summary.yaml` | Cross-table join |
| `pipelines/federated_join_and_insert.yaml` | Join with a CSV source, then `INSERT` the aggregate into SeekDB |
| `pipelines/fts_search.yaml` | Native FTS via `seekdb_fts` |
| `pipelines/fts_search_with_filter.yaml` | FTS + WHERE pushdown |
| `pipelines/knn_search.yaml` | KNN vector search via `seekdb_knn` (literal vector) |
| `pipelines/knn_search_by_seed.yaml` | KNN using an existing row's embedding as the query vector (scalar subquery) |
| `pipelines/seed_doc_embed.yaml` | INSERT a row into `docs_embed`, embedding content inline via `candle()` |
| `pipelines/knn_search_by_text.yaml` | KNN driven by a text query, embedded inline via `candle()` (embedding UDF) |

## Detailed Examples

### Point lookup

```bash
curl -X POST http://localhost:8080/seekdb-query-user-by-id/execute \
  -H "Content-Type: application/json" \
  -d '{"user_id": 1}'
```

Expected response (abbreviated):

```json
{
  "success": true,
  "data": [{"id": 1, "name": "Alice Smith", "email": "alice@example.com"}]
}
```

### Full-text search (native FTS)

```bash
curl -X POST http://localhost:8080/seekdb-fts-search/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "machine learning", "limit": 5}'
```

The response contains rows sorted by `_score DESC`:

```json
{
  "success": true,
  "data": [
    {"title": "Intro to Machine Learning",    "category": "ai",       "_score": 0.87},
    {"title": "Deep Learning Advances",       "category": "research", "_score": 0.61},
    {"title": "Neural Network Architectures", "category": "ai",       "_score": 0.49}
  ]
}
```

Scores are the raw MySQL `MATCH(...) AGAINST(... IN NATURAL LANGUAGE MODE)`
values; higher means more relevant.

### KNN search (native HNSW)

```bash
curl -X POST http://localhost:8080/seekdb-knn-search/execute \
  -H "Content-Type: application/json" \
  -d '{"query_vec": [1.0, 0.0, 0.0, 0.0], "k": 3}'
```

Expected response:

```json
{
  "success": true,
  "data": [
    {"title": "doc-a", "category": "electronics", "_score": 0.0},
    {"title": "doc-d", "category": "electronics", "_score": 1.0},
    {"title": "doc-e", "category": "books",       "_score": 1.25}
  ]
}
```

`_score` is the raw L2 distance — **lower is more similar**, matching
`pg_knn` / `sqlite_knn`.

### KNN search with an embedding UDF (text → vector inline)

`seekdb_knn` accepts three shapes for its query-vector argument:

1. **Literal array** — `[1.0, 0.0, 0.0, 0.0]` (see `knn_search.yaml`).
2. **Scalar subquery** — `(SELECT embedding FROM docs WHERE id = {seed_id})`
   (see `knn_search_by_seed.yaml`).
3. **Scalar function call** — any UDF returning `List<Float32>`, including
   Skardi's `candle()` / `remote_embed()` embedding UDFs. The UDF is
   evaluated once at execution time and the resulting vector is fed into
   SeekDB's HNSW probe.

The walk-through below demonstrates the third shape end-to-end using
[bge-small-en-v1.5](https://huggingface.co/BAAI/bge-small-en-v1.5) (384-dim)
via the `candle()` UDF.

**1. Create the 384-dim companion table.** From a SQL client
(`docker exec -i seekdb mysql -h 127.0.0.1 -P 2881 -u "root@sys"` works
if you don't have `mysql` on the host):

```sql
USE mydb;
CREATE TABLE docs_embed (
    id INT PRIMARY KEY AUTO_INCREMENT,
    title VARCHAR(200) NOT NULL,
    category VARCHAR(50) NOT NULL,
    content TEXT NOT NULL,
    embedding VECTOR(384),
    VECTOR INDEX idx_docs_embed (embedding) WITH (TYPE = HNSW, DISTANCE = COSINE)
);
```

**2. Download the embedding model** (single-directory layout the `candle()`
UDF expects):

```bash
pip install huggingface_hub
python -c "
from huggingface_hub import hf_hub_download
import os
model_dir = 'models/generated/bge-small-en-v1.5'
os.makedirs(model_dir, exist_ok=True)
for f in ['model.safetensors', 'config.json', 'tokenizer.json']:
    hf_hub_download('BAAI/bge-small-en-v1.5', f, local_dir=model_dir)
"
```

**3. Start the server with `--features candle`** so the `candle` UDF is
registered alongside the SeekDB provider:

```bash
export SEEKDB_USER="root@sys"
export SEEKDB_PASSWORD=""

cargo run --bin skardi-server --features candle -- \
  --ctx docs/seekdb/ctx_seekdb_demo.yaml \
  --pipeline docs/seekdb/pipelines/ \
  --port 8080
```

**4. Seed rows via `seed_doc_embed.yaml`.** The INSERT calls `candle()`
inline and formats the resulting `List<Float32>` as a pgvector-style
string so SeekDB's `VECTOR` column can parse it:

```bash
for payload in \
  '{"title":"Neural Networks","category":"ai","content":"deep learning with backpropagation and gradient descent"}' \
  '{"title":"Quantum Computing","category":"physics","content":"qubits superposition and quantum entanglement"}' \
  '{"title":"Espresso Brewing","category":"coffee","content":"pulling a shot grinding beans and steaming milk"}'
do
  curl -sS -X POST http://localhost:8080/seekdb-seed-doc-embed/execute \
    -H "Content-Type: application/json" -d "$payload"
done
```

**5. Query by text via `knn_search_by_text.yaml`.** `seekdb_knn` receives
`candle(...)` as its third argument, plans it as a scalar-function
expression, and embeds the query at execution time:

```bash
curl -sS -X POST http://localhost:8080/seekdb-knn-search-by-text/execute \
  -H "Content-Type: application/json" \
  -d '{"text": "training deep learning models", "k": 3}'
```

The top hit is the "Neural Networks" row (smallest cosine distance); the
quantum and coffee rows rank further out.

The pipeline body is:

```sql
SELECT id, title, category, _score
FROM seekdb_knn('docs_embed', 'embedding',
    candle('models/generated/bge-small-en-v1.5', {text}),
    'cosine', {k})
ORDER BY _score ASC
```

## Connection Options

| Option | Type | Default | Purpose |
|---|---|---|---|
| `table` | string | — | Table name (required in table mode). |
| `schema` | string | — | Database/schema name (optional). |
| `allowed_schemas` | string (csv) | — | Comma-separated schema allow-list (catalog mode). |
| `user_env` | string | — | Env var holding the SeekDB username. |
| `pass_env` | string | — | Env var holding the SeekDB password. |
| `ssl_mode` | string | `disabled` | `disabled` / `preferred` / `required`. |

The connection URL uses the `mysql://` scheme. Default port is `2881` when
the URL omits one; that differs from the MySQL provider's `3306`.
