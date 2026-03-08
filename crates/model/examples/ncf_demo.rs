use std::sync::Arc;

use anyhow::Result;
use arrow::array::{Float32Array, Int64Array};
use datafusion::prelude::SessionContext;
use model::OnnxModelRegistry;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Create a DataFusion context
    let mut ctx = SessionContext::new();

    // 2. Register the movie_embeddings Lance dataset as a table
    source::providers::lance::register_lance_table(
        &mut ctx,
        "movie_embeddings",
        "data/movie_embeddings.lance",
        None,
    )
    .await?;

    println!("Registered movie_embeddings.lance table");

    // 3. Register the onnx_predict UDF (no model pre-registration needed)
    let registry = Arc::new(OnnxModelRegistry::new());
    registry.register_onnx_predict_udf(&mut ctx);

    println!("Registered onnx_predict UDF");

    // 4. Query using onnx_predict — model path inline, loaded lazily on first call
    let sql = r#"
        SELECT
            movie_id,
            onnx_predict('models/ncf.onnx', CAST(1 AS BIGINT), movie_id) AS prediction_score
        FROM movie_embeddings
        WHERE movie_id < 3700
        ORDER BY prediction_score DESC
        LIMIT 10
    "#;

    println!("\nExecuting query:");
    println!("{}", sql);

    // 5. Execute the query
    let df = ctx.sql(sql).await?;
    let batches = df.collect().await?;

    // 6. Extract and print the results
    println!("\nMovie Prediction Results:");
    println!("{:-<60}", "");
    println!("{:<15} | {:<20}", "Movie ID", "Prediction Score");
    println!("{:-<60}", "");

    for batch in &batches {
        let movie_ids = batch
            .column(batch.schema().index_of("movie_id")?)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("movie_id should be Int64Array");

        let predictions = batch
            .column(batch.schema().index_of("prediction_score")?)
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("prediction_score should be Float32Array");

        for i in 0..batch.num_rows() {
            println!(
                "{:<15} | {:<20.6}",
                movie_ids.value(i),
                predictions.value(i)
            );
        }
    }

    println!("{:-<60}", "");
    println!("\nNCF demo completed successfully!");

    Ok(())
}
