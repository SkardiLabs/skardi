# Lance Vector Search & Full-Text Search Demo

This demo showcases Skardi's integration with Lance for high-performance vector similarity search and BM25-scored full-text search. It demonstrates:
- Native ANN (Approximate Nearest Neighbor) search using Lance's Scanner.nearest() API
- Explicit KNN search via the `lance_knn` table function
- Full-text search via the `lance_fts` table function with inverted indexes
- Seamless SQL integration for both vector and text search queries

## Datasets

The demo includes Lance datasets under `data/`:

### vec_data.lance
General-purpose vector embeddings for similarity search:
- **id**: int64 - Unique identifier
- **vector**: fixed_size_list\<float\>[128] - 128-dimensional embedding vector
- **item_id**: int64 - Reference to associated item
- **revenue**: double - Revenue associated with the item

### test_data.lance
Text dataset with INVERTED FTS index for full-text search:
- **id**: int64 - Unique identifier
- **vector**: fixed_size_list\<float\>[128] - 128-dimensional embedding vector
- **item_id**: int64 - Reference to associated item
- **revenue**: double - Revenue associated with the item
- **description**: string - Text description (has INVERTED index with positions)
- **category**: string - Category label

To regenerate: `python scripts/prepare_fts_test_data.py`

### movie_embeddings.lance
Movie embeddings for recommendation pipelines:
- **movie_id**: int64 - Movie identifier
- **embedding**: fixed_size_list\<float\>[128] - 128-dimensional movie embedding

## Demo Components

| File | Description |
|------|-------------|
| `ctx_lance.yaml` | Context file registering `vec_data.lance` and `test_data.lance` |
| `pipelines/pipeline_lance.yaml` | KNN similarity search pipeline |
| `pipelines/pipeline_lance_fts.yaml` | Full-text search pipeline |

## How It Works

### lance_knn Table Function

Use the `lance_knn` table function for explicit KNN search:
```sql
SELECT * FROM lance_knn(table_name, vector_column, query_vector, k, [filter])
```

Parameters:
- `table_name`: Name of the Lance table (string)
- `vector_column`: Name of the embedding column (string)
- `query_vector`: Query vector as literal array or scalar subquery
- `k`: Number of nearest neighbors to retrieve from the ANN index (integer)
- `filter`: Optional Lance filter predicate (string)

Both `lance_knn` and `lance_fts` support standard SQL clauses:
- **WHERE** — predicates are pushed down to Lance for efficient metadata filtering
- **LIMIT** — applied after search + filtering to cap the final result set
- **Column projection** — only requested columns are returned

### Filter Pushdown

WHERE clause predicates are pushed down to Lance, so you can combine KNN search with metadata filters:
```sql
-- Find 50 nearest neighbors, filter to electronics, return top 5
SELECT id, category, _distance
FROM lance_knn('items', 'vector', (SELECT vector FROM items WHERE id = 1), 50)
WHERE category = 'electronics'
LIMIT 5
```

Note: `k` and `LIMIT` serve different purposes. `k` controls how many ANN candidates Lance retrieves from the index. `LIMIT` truncates the final result set after filtering. When using WHERE filters, set `k` higher than your desired result count to ensure enough candidates survive filtering.

### Query Execution

The table function directly calls Lance's KNN API:
```
lance_knn(...) -> LanceKnnExec -> Lance Scanner.nearest()
```

### Performance Benefits

- **Without optimization**: O(N * D + N log N) - full scan + sort
- **With Lance KNN**: O(k log N) - index-based ANN search
- **Typical speedup**: 10x-1000x for datasets with N > 100K vectors

| Dataset Size | Without Optimization | With Lance KNN | Speedup |
|--------------|---------------------|----------------|---------|
| 10K vectors  | ~50ms              | ~5ms           | 10x     |
| 100K vectors | ~500ms             | ~8ms           | 62x     |
| 1M vectors   | ~5000ms            | ~15ms          | 333x    |
| 10M vectors  | ~50000ms           | ~25ms          | 2000x   |

*Benchmarks: 128-dim vectors, k=10, IVF-PQ index, Intel Core i9*

## Running the Demo

### Start the Server

```bash
cargo run --bin skardi-server -- \
  --ctx demo/lance/ctx_lance.yaml \
  --pipeline demo/lance/pipelines/ \
  --port 8080
```

Expected output:
```
Starting Skardi Online Serving Pipeline Server
CLI Arguments parsed successfully
   Pipeline file: Some("demo/lance/pipeline_lance.yaml")
   Context file: Some("demo/lance/ctx_lance.yaml")
   Port: 8080
Server configuration loaded successfully
   Pipeline: lance-vector-similarity-search
   Data sources: 1
Server listening on 0.0.0.0:8080
```

### Example 1: Find Similar Items

Find items most similar to item with id=1:

```bash
curl -X POST http://localhost:8080/lance-vector-similarity-search/execute \
  -H "Content-Type: application/json" \
  -d '{
    "reference_id": 1,
    "min_revenue": null,
    "max_revenue": null
  }'
```

Response:
```json
{
  "success": true,
  "data": [
    {
      "id": 42,
      "item_id": 1337,
      "revenue": 2500.50,
      "distance": 0.125
    },
    {
      "id": 89,
      "item_id": 2048,
      "revenue": 1800.25,
      "distance": 0.187
    }
  ],
  "rows": 10,
  "execution_time_ms": 8
}
```

### Example 2: Filtered Similarity Search

Find similar items with revenue constraints:

```bash
curl -X POST http://localhost:8080/lance-vector-similarity-search/execute \
  -H "Content-Type: application/json" \
  -d '{
    "reference_id": 1,
    "min_revenue": 1000.0,
    "max_revenue": 5000.0
  }'
```

### Example 3: Direct Vector Search

Instead of looking up a reference vector by ID, you can pass the query vector directly in the request. This is useful when you already have an embedding from an external model.

Pipeline: `pipelines/pipeline_lance_direct_vector.yaml`

```bash
curl -X POST http://localhost:8080/lance-direct-vector-search/execute \
  -H "Content-Type: application/json" \
  -d '{
    "query_vector": [0.0, 16.0, 35.0, 5.0, 32.0, ...]
  }'
```

The `query_vector` parameter accepts a JSON array of floats matching the vector dimension of the dataset (128 for the demo dataset). The server converts the array to a SQL literal for `lance_knn`.

A test script is provided to read a real vector from the dataset and send it:

```bash
python demo/lance/test_direct_vector_search.py
```

## KNN Pipeline Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `reference_id` | integer | Yes | ID of the reference item to find similar items for |
| `min_revenue` | double | No | Minimum revenue filter via WHERE pushdown (null = no filter) |
| `max_revenue` | double | No | Maximum revenue filter via WHERE pushdown (null = no filter) |

## SQL Query Patterns

### Basic KNN Search

Find similar items using a subquery for the reference vector:
```sql
SELECT knn.id, knn.item_id, knn._distance as dist
FROM lance_knn(
  'sift_items',
  'vector',
  (SELECT vector FROM sift_items WHERE id = 1),
  10
) knn
WHERE knn.id != 1
```

### Using CTE for Reference Vector

```sql
WITH ref AS (
  SELECT vector FROM sift_items WHERE id = 1
)
SELECT knn.id, knn.item_id, knn._distance
FROM lance_knn('sift_items', 'vector', (SELECT vector FROM ref), 10) knn
WHERE knn.id != 1
```

### With Inline Filter Parameter

The optional 5th argument applies a Lance filter predicate during ANN retrieval:
```sql
SELECT *
FROM lance_knn(
  'sift_items',
  'vector',
  (SELECT vector FROM sift_items WHERE id = 1),
  10,
  'revenue > 1000'
)
```

### With WHERE Clause Filter Pushdown

WHERE clause predicates are pushed down to Lance for efficient post-retrieval filtering:
```sql
SELECT id, category, _distance
FROM lance_knn(
  'sift_items',
  'vector',
  (SELECT vector FROM sift_items WHERE id = 1),
  50
)
WHERE category = 'electronics' AND revenue > 1000
LIMIT 10
```

### Combining Inline Filter and WHERE Clause

Both filters are combined with AND:
```sql
SELECT id, category, _distance
FROM lance_knn(
  'sift_items',
  'vector',
  (SELECT vector FROM sift_items WHERE id = 1),
  50,
  'revenue > 500'
)
WHERE category = 'electronics'
LIMIT 5
```

### Movie Recommendation (Federated: Lance + PostgreSQL)

The `pipeline_movie_recommendation.yaml` demonstrates a more advanced use case — finding similar movies via Lance KNN, then ranking them with an ONNX model, and joining with a PostgreSQL movies table for metadata:

```sql
WITH knn_results AS (
  SELECT knn.movie_id
  FROM lance_knn(
    'movie_embeddings',
    'embedding',
    (SELECT embedding FROM movie_embeddings WHERE movie_id = (SELECT movie_id FROM movies WHERE title = {last_watched_movie})),
    10
  ) knn
)
SELECT m.title, m.genres, rr.prediction_score
FROM ranked_recommendations rr
JOIN movies m ON rr.movie_id = m.movie_id
ORDER BY rr.prediction_score DESC
LIMIT {top_n}
```

## Full-Text Search

### lance_fts Table Function

Use the `lance_fts` table function for BM25-scored full-text search:
```sql
SELECT * FROM lance_fts(table_name, text_column, query, limit)
```

Parameters:
- `table_name`: Name of the Lance table with an INVERTED index (string)
- `text_column`: Name of the text column to search (string)
- `query`: Search query string (string)
- `limit`: Maximum number of results (integer)

Results include a `_score` column (Float32) where higher values = more relevant.

### Query Syntax

| Syntax | Type | Description |
|--------|------|-------------|
| `foo bar` | Term (OR) | Matches documents containing any term |
| `+foo bar` | Term (AND) | All terms must be present |
| `"foo bar"` | Phrase | Exact phrase match (requires index with positions) |
| `foo~` / `foo~2` | Fuzzy | Typo-tolerant matching |
| `+foo -bar` | Boolean | MUST contain foo, MUST NOT contain bar |
| `+foo bar` | Boolean | MUST contain foo, SHOULD contain bar |

### FTS Filter Pushdown

WHERE clause predicates are pushed down to Lance for efficient metadata filtering:
```sql
SELECT id, description, _score
FROM lance_fts('fts_data', 'description', 'premium wireless', 10)
WHERE category = 'electronics' AND revenue > 1000
LIMIT 5
```

### Running the FTS Demo

```bash
cargo run --bin skardi-server -- \
  --ctx demo/lance/ctx_lance.yaml \
  --pipeline demo/lance/pipelines/ \
  --port 8080
```

### Example: Basic Text Search

```bash
curl -X POST http://localhost:8080/lance-full-text-search/execute \
  -H "Content-Type: application/json" \
  -d '{
    "search_query": "premium wireless",
    "category": null,
    "limit": 10
  }'
```

Response:
```json
{
  "success": true,
  "data": [
    {
      "id": 0,
      "item_id": 42,
      "revenue": 1200.50,
      "description": "A premium wireless organic speaker charger for electronics enthusiasts.",
      "category": "electronics",
      "relevance": 4.25
    }
  ],
  "rows": 10,
  "execution_time_ms": 5
}
```

### Example: Search with Category Filter

```bash
curl -X POST http://localhost:8080/lance-full-text-search/execute \
  -H "Content-Type: application/json" \
  -d '{
    "search_query": "umbrella",
    "category": "outdoor",
    "limit": 5
  }'
```

### Example: Phrase Search

```bash
curl -X POST http://localhost:8080/lance-full-text-search/execute \
  -H "Content-Type: application/json" \
  -d '{
    "search_query": "\"train to boston\"",
    "category": null,
    "limit": 10
  }'
```

### Creating an INVERTED Index

To use `lance_fts`, your Lance dataset needs an INVERTED index on the text column. Create one using the Python SDK:

```python
import lance

ds = lance.dataset("data/my_dataset.lance")
ds.create_scalar_index(
    "text_column",
    index_type="INVERTED",
    with_position=True,  # Required for phrase search
)
```

See `scripts/prepare_fts_test_data.py` for a complete example.

### FTS Pipeline Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `search_query` | string | Yes | Search query (supports term, phrase, fuzzy, boolean syntax) |
| `category` | string | No | Category filter (null = no filter) |
| `limit` | integer | Yes | Maximum number of results |

### Troubleshooting FTS

#### "No results returned"
Ensure the Lance dataset has an INVERTED index on the target column. Without an index, FTS queries return empty results.

#### "Phrase search returns unexpected results"
Verify the INVERTED index was created with `with_position=True`. Without positions, phrase queries fall back to term matching.

## Creating Your Own Vector Search Pipeline

### 1. Create Context Configuration

```yaml
data_sources:
  - name: "my_vectors"
    type: "lance"
    path: "data/my_vectors.lance/"
    description: "My vector embeddings"
```

### 2. Create Pipeline Configuration

```yaml
metadata:
  name: my-vector-search
  version: 1.0.0

query: |
  SELECT
    knn.id,
    knn.item_id,
    knn._distance as similarity
  FROM lance_knn(
    'my_vectors',
    'vector',
    (SELECT vector FROM my_vectors WHERE id = {query_id}),
    {k}
  ) knn
  WHERE knn.id != {query_id}
```

### 3. Run Your Pipeline

```bash
cargo run --bin skardi-server -- \
  --ctx ctx_my_vectors.yaml \
  --pipeline pipeline_my_search.yaml \
  --port 8080
```

## Monitoring Execution

Enable debug logs to see KNN execution:

```bash
RUST_LOG=debug cargo run --bin skardi-server -- \
  --ctx demo/lance/ctx_lance.yaml \
  --pipeline demo/lance/pipelines/ \
  --port 8080
```

Look for:
```
INFO  source::lance::knn_table_function: Registering Lance table functions
INFO  LanceKnnExec: Executing KNN search
```

## Troubleshooting

### "lance_knn: table 'xxx' not found in registry"
Ensure your context file uses `type: "lance"` for the data source and the table name matches.

### "lance_knn: subquery must return exactly one column"
The query vector subquery must return a single column containing the vector.

### "lance_knn: query_vector must be literal array or scalar subquery"
The third argument must be either a literal array `[0.1, 0.2, ...]` or a scalar subquery `(SELECT vector FROM ...)`.

### "Distance values seem incorrect"
Distance metric is determined by the Lance index. Check your index configuration:
- L2 (Euclidean): Default for most cases
- Cosine: Better for normalized vectors
- Dot Product: For inner product similarity

## Additional Resources

- [Lance Documentation](https://lancedb.github.io/lance/)
- [DataFusion Integration](https://docs.rs/datafusion/)
