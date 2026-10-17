//! `fetch_table_columns` against a live Postgres whose `information_schema`
//! integer columns come back at an unexpected wire width.
//!
//! On Postgres itself `numeric_precision`/`numeric_scale` are
//! `cardinal_number` (INT4). TiDB Cloud's Postgres mode returns INT8 for the
//! same columns, and before the width-tolerant decoder every table on such a
//! source failed schema inference — which fails the whole engine's boot,
//! since one failed registration fails boot. Postgres cannot be told to
//! serve `information_schema` at a different width, so this test proves the
//! real INT4 path through `fetch_table_columns` and then the decoder itself
//! on rows carrying the same column names with explicit INT8/INT2 casts.
//!
//! Gated on a live URL so the default suite stays hermetic:
//!
//! ```bash
//! docker run -d --name skardi-pg-width -p 127.0.0.1:15433:5432 \
//!   -e POSTGRES_PASSWORD=pw postgres:16-alpine
//! SKARDI_PG_LIVE_URL='postgres://postgres:pw@127.0.0.1:15433/postgres' \
//!   cargo test -p skardi --test pg_information_schema_int_width_live
//! ```

use arrow::datatypes::DataType;
use skardi::sources::providers::sqlx::pg::knn_table_function::{
    fetch_table_columns, try_get_opt_int,
};
use sqlx::PgPool;

/// `SKARDI_PG_LIVE_URL` for a hand run; otherwise CI's Postgres service, the
/// way `fts_table_function`'s integration tests reach it (`PG_USER` /
/// `PG_PASSWORD` against `127.0.0.1:5432/mydb`); otherwise skip.
fn live_url() -> Option<String> {
    if let Ok(url) = std::env::var("SKARDI_PG_LIVE_URL")
        && !url.trim().is_empty()
    {
        return Some(url);
    }
    if let (Ok(user), Ok(pass)) = (std::env::var("PG_USER"), std::env::var("PG_PASSWORD")) {
        return Some(format!(
            "postgres://{user}:{pass}@127.0.0.1:5432/mydb?sslmode=disable"
        ));
    }
    eprintln!("SKARDI_PG_LIVE_URL and PG_USER/PG_PASSWORD unset — skipping");
    None
}

#[tokio::test]
async fn numeric_precision_decodes_at_int4_int8_and_int2_widths() {
    let Some(url) = live_url() else { return };
    let pool = PgPool::connect(&url).await.expect("connect");

    // The real table: Postgres's own INT4 path must keep working.
    sqlx::raw_sql(
        "DROP SCHEMA IF EXISTS width_probe CASCADE;
         CREATE SCHEMA width_probe;
         CREATE TABLE width_probe.orders (
             id BIGINT PRIMARY KEY,
             amount NUMERIC(12, 3),
             note TEXT
         );",
    )
    .execute(&pool)
    .await
    .expect("seed");

    let columns = fetch_table_columns(&pool, "width_probe", "orders")
        .await
        .expect("INT4 width, the Postgres default");
    assert_eq!(
        columns,
        vec![
            ("id".to_string(), DataType::Int64),
            ("amount".to_string(), DataType::Decimal128(12, 3)),
            ("note".to_string(), DataType::Utf8),
        ]
    );

    // Now the widths a Postgres-compatible server may use. Postgres will not
    // serve `information_schema.columns` wider, so stand in for it: the
    // decoder is exercised on rows whose `numeric_precision` is INT8 and
    // `numeric_scale` is INT2, read with the same query shape.
    let row = sqlx::query(
        "SELECT 12::int8 AS numeric_precision, 3::int2 AS numeric_scale, \
                NULL::int8 AS absent, 9999999999::int8 AS too_wide",
    )
    .fetch_one(&pool)
    .await
    .expect("probe row");
    assert_eq!(
        try_get_opt_int(&row, "numeric_precision").unwrap(),
        Some(12),
        "INT8 must decode"
    );
    assert_eq!(
        try_get_opt_int(&row, "numeric_scale").unwrap(),
        Some(3),
        "INT2 must decode"
    );
    assert_eq!(try_get_opt_int(&row, "absent").unwrap(), None);
    assert_eq!(
        try_get_opt_int(&row, "too_wide").unwrap(),
        None,
        "a value no precision can have is absent, not an error"
    );

    sqlx::raw_sql("DROP SCHEMA width_probe CASCADE")
        .execute(&pool)
        .await
        .expect("cleanup");
}

/// A column that is not an integer at any width is a hard error, not a
/// silent `None`: a server handing back `text` or `bool` for
/// `numeric_precision` is broken in a way the caller must hear about, and
/// the message has to name the column so the failing table is findable in
/// the registration log.
#[rstest::rstest]
#[case::text("'12'::text")]
#[case::bool("true::bool")]
#[case::float("12.0::float8")]
#[case::numeric("12::numeric")]
#[tokio::test]
async fn non_integer_column_is_an_error_naming_the_column(#[case] expr: &str) {
    let Some(url) = live_url() else { return };
    let pool = PgPool::connect(&url).await.expect("connect");

    let row = sqlx::query(&format!("SELECT {expr} AS numeric_precision"))
        .fetch_one(&pool)
        .await
        .expect("probe row");

    let err = try_get_opt_int(&row, "numeric_precision")
        .expect_err("a non-integer column must not decode as an integer");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("decoding 'numeric_precision' as any of INT2/INT4/INT8"),
        "error must name the column and the widths tried; got: {msg}"
    );
    assert!(
        msg.contains("mismatched types"),
        "the INT4 decode error must be kept as the cause; got: {msg}"
    );
}
