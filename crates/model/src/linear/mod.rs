use crate::model::Model;
use anyhow::{Result, anyhow};
use arrow::{
    array::{ArrayRef, Float32Array, Float32Builder},
    datatypes::DataType,
};
use datafusion::{logical_expr::ColumnarValue, prelude::create_udf};
use std::sync::Arc;

/// Simple in-memory linear regression model: y = coef * x + intercept
pub struct LinearModel {
    coef: f32,
    intercept: f32,
}

impl LinearModel {
    pub fn new(coef: f32, intercept: f32) -> Self {
        Self { coef, intercept }
    }

    /// Register this linear model as a DataFusion UDF.
    pub fn register_udf(
        self: Arc<Self>,
        ctx: &mut datafusion::prelude::SessionContext,
        udf_name: &str,
        input_types: Vec<DataType>,
        return_type: DataType,
    ) {
        let model = self.clone();
        let func = move |args: &[ColumnarValue]| -> std::result::Result<
            ColumnarValue,
            datafusion::error::DataFusionError,
        > {
            use crate::converter::columnar_values_to_arrayrefs;

            let arrs = columnar_values_to_arrayrefs(args)
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

            let result_arr = model
                .predict(&arrs)
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

            Ok(ColumnarValue::Array(Arc::new(result_arr)))
        };

        let udf = create_udf(
            udf_name,
            input_types,
            return_type,
            datafusion::logical_expr::Volatility::Immutable,
            Arc::new(func),
        );
        ctx.register_udf(udf);
    }
}

impl Model for LinearModel {
    fn predict(&self, args: &[ArrayRef]) -> Result<Float32Array> {
        let arr = &args[0];
        if let Some(f32_arr) = arr.as_any().downcast_ref::<Float32Array>() {
            let len = f32_arr.len();
            let mut b = Float32Builder::new();
            for i in 0..len {
                let x = f32_arr.value(i);
                b.append_value(self.coef * x + self.intercept);
            }
            Ok(b.finish())
        } else {
            Err(anyhow!("Invalid input"))
        }
    }
}

#[cfg(test)]
mod test {
    use anyhow::Result;
    use arrow::{
        array::{ArrayRef, Float32Array, RecordBatch},
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::{datasource::MemTable, prelude::SessionContext};
    use std::sync::Arc;

    use crate::linear::LinearModel;

    #[tokio::test]
    async fn test_linear_model_udf() -> Result<()> {
        // Create a linear model y = 2x + 1
        let model = Arc::new(LinearModel::new(2.0, 1.0));

        // DataFusion context and register UDF
        let mut ctx = SessionContext::new();
        model.register_udf(
            &mut ctx,
            "lin_udf",
            vec![DataType::Float32],
            DataType::Float32,
        );

        // Create in-memory RecordBatch
        let schema = Schema::new(vec![Field::new("x", DataType::Float32, false)]);
        let x_array = Float32Array::from(vec![0.0, 1.5, 2.0]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(x_array) as ArrayRef],
        )?;
        let table = MemTable::try_new(Arc::new(schema), vec![vec![batch]])?;
        ctx.register_table("t", Arc::new(table))?;

        // Query with UDF
        let df = ctx.sql("SELECT x, lin_udf(x) AS y FROM t").await?;
        let results = df.collect().await?;

        // Validate
        let mut inputs = Vec::new();
        let mut preds = Vec::new();
        for batch in results {
            let col_x = batch
                .column(0)
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap();
            let col_y = batch
                .column(1)
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                inputs.push(col_x.value(i));
                preds.push(col_y.value(i));
            }
        }
        assert_eq!(
            preds,
            inputs.iter().map(|&x| 2.0 * x + 1.0).collect::<Vec<_>>()
        );
        Ok(())
    }
}
