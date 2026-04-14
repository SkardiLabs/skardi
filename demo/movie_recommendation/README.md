# Movie Recommendation Demo

This demo builds a personalized movie recommendation system using Lance vector search and ONNX model inference. It finds similar movies via KNN embeddings, then re-ranks them with a Neural Collaborative Filtering (NCF) model — all in a single SQL query.

## How It Works

```
User watches "Toy Story"
        │
        ▼
Lance KNN  ─── find 10 similar movies by embedding
        │
        ▼
onnx_predict  ─── score each candidate for the user with NCF
        │
        ▼
Top-N personalized recommendations
```

## Prerequisites

1. Lance dataset with movie embeddings at `data/movie_embeddings.lance`
2. Movies CSV at `docs/sample_data/movies.csv`
3. NCF model at `models/ncf.onnx`
4. Skardi server built with the `embedding` feature:
   ```bash
   cargo build --release -p skardi-server --features embedding
   ```

## Data Sources

```yaml
data_sources:
  - name: "movies"
    type: "csv"
    path: "docs/sample_data/movies.csv"

  - name: "movie_embeddings"
    type: "lance"
    path: "data/movie_embeddings.lance"
```

## Pipeline

```sql
-- Step 1: Find the movie by title
WITH last_watched AS (
  SELECT movie_id, title
  FROM movies
  WHERE title = {last_watched_movie}
  LIMIT 1
),
-- Step 2: Find 10 similar movies via Lance KNN
knn_results AS (
  SELECT knn.movie_id
  FROM lance_knn(
    'movie_embeddings',
    'embedding',
    (SELECT embedding FROM movie_embeddings
     WHERE movie_id = (SELECT movie_id FROM last_watched)),
    10
  ) knn
  WHERE knn.movie_id != (SELECT movie_id FROM last_watched)
),
-- Step 3: Score each candidate with the NCF ONNX model
ranked_recommendations AS (
  SELECT
    kr.movie_id,
    onnx_predict('models/ncf.onnx',
      CAST({user_id} AS BIGINT),
      CAST(kr.movie_id AS BIGINT)
    ) AS prediction_score
  FROM knn_results kr
)
-- Step 4: Join with movie metadata and return top results
SELECT
  m.movie_id, m.title, m.genres, m.year,
  rr.prediction_score
FROM ranked_recommendations rr
JOIN movies m ON rr.movie_id = m.movie_id
ORDER BY rr.prediction_score DESC
LIMIT {top_n}
```

## Running the Demo

```bash
cargo run --bin skardi-server --features embedding -- \
  --ctx demo/movie_recommendation/ctx_movie_recommendation.yaml \
  --pipeline demo/movie_recommendation/pipelines/ \
  --port 8080
```

## Execute

```bash
curl -X POST http://localhost:8080/movie-recommendation-pipeline/execute \
  -H "Content-Type: application/json" \
  -d '{
    "last_watched_movie": "Toy Story",
    "user_id": 42,
    "top_n": 5
  }' | jq .
```

## Pipeline Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `last_watched_movie` | string | Title of the seed movie |
| `user_id` | integer | User ID for personalized NCF scoring |
| `top_n` | integer | Number of recommendations to return |

## Available Models

| Model | Description | Inputs |
|-------|-------------|--------|
| `ncf.onnx` | Neural Collaborative Filtering | user_id (INT64), item_id (INT64) |
| `TinyTimeMixer.onnx` | Time-series forecasting | aggregated float sequences |

For the `onnx_predict` UDF reference, see [docs/onnx_predict.md](../../docs/onnx_predict.md).
