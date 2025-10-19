#![cfg(feature = "chrono")]

use serde::{Deserialize, Serialize};
use clickhouse::Row;
use chrono::{DateTime, Utc};
use clickhouse::serde::chrono::DateTimeParam;

#[tokio::test]
async fn smoke() {
    let client = prepare_database!();

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Row)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::chrono::datetime")]
        dt: DateTime<Utc>,
    }

    client
        .query(
            "
            CREATE TABLE test(
                dt DateTime
            ) ENGINE = MergeTree ORDER BY dt
        ",
        )
        .execute()
        .await
        .unwrap();

    let dt = DateTime::parse_from_rfc3339("2024-01-02T15:04:05Z")
        .unwrap()
        .with_timezone(&Utc);

    let original_row = MyRow { dt };

    let mut insert = client.insert::<MyRow>("test").await.unwrap();
    insert.write(&original_row).await.unwrap();
    insert.end().await.unwrap();

    let (row, row_dt_str) = client
        .query("SELECT ?fields, toString(dt) FROM test")
        .fetch_one::<(MyRow, String)>()
        .await
        .unwrap();

    assert_eq!(row, original_row);
    assert_eq!(row_dt_str, dt.format("%Y-%m-%d %H:%M:%S").to_string());
}

#[tokio::test]
async fn datetime_param_basic() {
    let client = prepare_database!();

    #[derive(Debug, Deserialize, Row)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::chrono::datetime")]
        dt: DateTime<Utc>,
    }

    let dt = DateTime::parse_from_rfc3339("2024-06-15T10:30:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let rows = client
        .query("SELECT {dt:DateTime} AS dt")
        .param("dt", DateTimeParam(dt))
        .fetch_all::<MyRow>()
        .await
        .unwrap();

    assert_eq!(rows[0].dt, dt);
}

#[tokio::test]
async fn datetime_param_multiple() {
    let client = prepare_database!();

    #[derive(Debug, Deserialize, Row)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::chrono::datetime")]
        start: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime")]
        end: DateTime<Utc>,
    }

    let start = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let end = DateTime::parse_from_rfc3339("2024-12-31T23:59:59Z")
        .unwrap()
        .with_timezone(&Utc);

    let rows = client
        .query("SELECT {start:DateTime} AS start, {end:DateTime} AS end")
        .param("start", DateTimeParam(start))
        .param("end", DateTimeParam(end))
        .fetch_all::<MyRow>()
        .await
        .unwrap();

    assert_eq!(rows[0].start, start);
    assert_eq!(rows[0].end, end);
}

#[tokio::test]
async fn datetime_param_where_clause() {
    let client = prepare_database!();

    #[derive(Debug, Deserialize, Row)]
    struct MyRow {
        result: u8,
    }

    let dt = DateTime::parse_from_rfc3339("2024-06-01T12:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let rows = client
        .query("SELECT 1 AS result WHERE {dt:DateTime} = toDateTime('2024-06-01 12:00:00')")
        .param("dt", DateTimeParam(dt))
        .fetch_all::<MyRow>()
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].result, 1);
}
