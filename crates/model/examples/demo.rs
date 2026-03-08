use std::sync::Arc;

use anyhow::Result;
use arrow::array::{Array, Float32Array, ListArray};
use datafusion::prelude::{CsvReadOptions, SessionContext};
use model::OnnxModelRegistry;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Create a DataFusion context
    let mut ctx = SessionContext::new();

    // 2. Register the ETTh1 CSV as a table named "etth1"
    ctx.register_csv(
        "etth1",
        "data/ETTh1.csv",
        CsvReadOptions::new().has_header(true),
    )
    .await?;

    // 3. Register the onnx_predict UDF (no model pre-registration needed)
    let registry = Arc::new(OnnxModelRegistry::new());
    registry.register_onnx_predict_udf(&mut ctx);

    // 4. Build SQL: model path inline, loaded lazily on first call
    let sql = r#"
        SELECT
          onnx_predict(
            'models/TinyTimeMixer.onnx',
            array_agg(etth1."HUFL"),
            array_agg(etth1."HULL"),
            array_agg(etth1."MUFL"),
            array_agg(etth1."MULL"),
            array_agg(etth1."LUFL"),
            array_agg(etth1."LULL"),
            array_agg(etth1."OT")
          ) AS forecast
        FROM (
          SELECT *
          FROM etth1
          LIMIT 512
        ) etth1
    "#;

    // 5. Execute the query
    let df = ctx.sql(sql).await?;
    let batches = df.collect().await?;

    // 6. Extract and print the prediction array
    for batch in &batches {
        let col = batch
            .column(batch.schema().index_of("forecast")?)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("Forecast column should be ListArray");

        if col.len() > 0 {
            let forecast_array = col.value(0);
            if let Some(float_arr) = forecast_array.as_any().downcast_ref::<Float32Array>() {
                println!(
                    "Forecast values (length {}): {:?}",
                    float_arr.len(),
                    float_arr.values()
                );
            } else {
                println!(
                    "Unexpected forecast array type: {:?}",
                    forecast_array.data_type()
                );
            }
        }
    }
    Ok(())
}
