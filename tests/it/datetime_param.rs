#![cfg(feature = "chrono")]

use serde::{Deserialize, Serialize};
use clickhouse::Row;
use chrono::{DateTime, Utc};

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
        .datetime_param("start", start)
        .datetime_param("end", end)
        .fetch_all::<MyRow>()
        .await
        .unwrap();

    assert_eq!(rows[0].start, start);
    assert_eq!(rows[0].end, end);
}
