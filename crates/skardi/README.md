# skardi

High-performance query engine for AI and agents, powered by [Apache DataFusion](https://datafusion.apache.org/).

Query files, databases, data lakes, and vector stores with SQL — and serve results as parameterized pipelines.

[![Crates.io](https://img.shields.io/crates/v/skardi.svg)](https://crates.io/crates/skardi)
[![Docs.rs](https://docs.rs/skardi/badge.svg)](https://docs.rs/skardi)
[![License](https://img.shields.io/crates/l/skardi.svg)](https://github.com/SkardiLabs/skardi/blob/main/LICENSE)

## Modules

| Module | Description |
|--------|-------------|
| `engine` | SQL query execution engine backed by DataFusion |
| `pipeline` | Declarative SQL pipelines with parameter inference |
| `model` | ONNX model loading and inference (behind `embedding` feature) |
| `sources` | Data source connectors: CSV, Parquet, PostgreSQL, MySQL, SQLite, MongoDB, Redis, Iceberg, Lance |

## Installation

```toml
[dependencies]
skardi = "0.1"

# With embedding (ONNX, Candle, GGUF, remote embeddings)
skardi = { version = "0.1", features = ["embedding"] }
```

## Quick Start

### Execute a SQL query

```rust,no_run
use skardi::engine::datafusion::DataFusionEngine;
use skardi::engine::Engine;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ctx = SessionContext::new();
    ctx.register_csv("products", "data/products.csv", CsvReadOptions::new()).await?;

    let engine = DataFusionEngine::new(ctx);
    let result = engine.execute("SELECT * FROM products WHERE price < 100").await?;

    println!("{} rows returned", result.num_rows());
    Ok(())
}
```

### Load and run a pipeline

```rust,no_run
use skardi::pipeline::pipeline::{Pipeline, StandardPipeline};
use datafusion::prelude::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ctx = Arc::new(SessionContext::new());

    // Pipeline YAML defines a parameterized SQL query
    let pipeline = StandardPipeline::load_from_file("pipeline.yaml", ctx).await?;

    println!("Pipeline: {} v{}", pipeline.name(), pipeline.version());
    Ok(())
}
```

A pipeline YAML looks like this:

```yaml
metadata:
  name: product-search
  version: 1.0.0

query: |
  SELECT name, price FROM products
  WHERE ({brand} IS NULL OR brand = {brand})
    AND price < {max_price}
  LIMIT {limit}
```

Parameters are automatically inferred from `{placeholders}` in the SQL.

## Feature Flags

| Feature | Description |
|---------|-------------|
| `onnx` | Enables ONNX model inference via the `model` module |

## Further Reading

- [API Reference](https://docs.rs/skardi) — full rustdoc for all modules
- [GitHub Repository](https://github.com/SkardiLabs/skardi) — server, CLI, demos, and deployment guide
- [Crate Page](https://crates.io/crates/skardi)

## License

Apache-2.0
