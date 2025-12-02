use serde::{Deserialize, Serialize};
use clickhouse::Row;
use clickhouse::serde::qbit::QBit;

#[tokio::test]
async fn qbit_roundtrip() {
    let client = prepare_database!();

    #[derive(Debug, PartialEq, Serialize, Deserialize, Row)]
    struct MyRow {
        qb16: QBit,
        qb16_opt: Option<QBit>,
        qb32: QBit,
        qb32_opt: Option<QBit>,
        qb64: QBit,
        qb64_opt: Option<QBit>,
    }

    client
        .query(
            r#"
            CREATE TABLE test_qbit (
                qb16        QBit(BFloat16, 128),
                qb16_opt    Nullable(QBit(BFloat16, 128)),
                qb32        QBit(Float32, 256),
                qb32_opt    Nullable(QBit(Float32, 256)),
                qb64        QBit(Float64, 512),
                qb64_opt    Nullable(QBit(Float64, 512))
            ) ENGINE = MergeTree
            ORDER BY tuple()
            SETTINGS allow_experimental_qbit_type = 1;
            "#,
        )
        .execute()
        .await
        .unwrap();

    let qb16_data = QBit::from_data(vec![vec![1u8; 128]; 16]);
    let qb32_data = QBit::from_data(vec![vec![2u8; 256]; 32]);
    let qb64_data = QBit::from_data(vec![vec![3u8; 512]; 64]);

    let row = MyRow {
        qb16: qb16_data.clone(),
        qb16_opt: Some(qb16_data.clone()),
        qb32: qb32_data.clone(),
        qb32_opt: Some(qb32_data.clone()),
        qb64: qb64_data.clone(),
        qb64_opt: Some(qb64_data.clone()),
    };

    let mut insert = client.insert::<MyRow>("test_qbit").await.unwrap();
    insert.write(&row).await.unwrap();
    insert.end().await.unwrap();

    let fetched_rows: Vec<MyRow> = client
        .query("SELECT ?fields FROM test_qbit")
        .fetch_all()
        .await
        .unwrap();

    assert_eq!(fetched_rows.len(), 1);
    assert_eq!(fetched_rows[0], row);
}

#[tokio::test]
async fn qbit_array_roundtrip() {
    let client = prepare_database!();

    #[derive(Debug, PartialEq, Serialize, Deserialize, Row)]
    struct MyRow {
        qb_array: Vec<QBit>,
    }

    client
        .query(
            r#"
            CREATE TABLE test_qbit_array (
                qb_array        Array(QBit(Float32, 128))
            ) ENGINE = MergeTree
            ORDER BY tuple()
            SETTINGS allow_experimental_qbit_type = 1;
            "#,
        )
        .execute()
        .await
        .unwrap();

    let qb_element1 = QBit::from_data(vec![vec![1u8; 128]; 32]);
    let qb_element2 = QBit::from_data(vec![vec![2u8; 128]; 32]);
    let qb_array_data = vec![qb_element1, qb_element2];

    let row = MyRow {
        qb_array: qb_array_data.clone(),
    };

    let mut insert = client.insert::<MyRow>("test_qbit_array").await.unwrap();
    insert.write(&row).await.unwrap();
    insert.end().await.unwrap();

    let fetched_rows: Vec<MyRow> = client
        .query("SELECT ?fields FROM test_qbit_array")
        .fetch_all()
        .await
        .unwrap();

    assert_eq!(fetched_rows.len(), 1);
    assert_eq!(fetched_rows[0], row);
}

#[tokio::test]
async fn qbit_dimensions() {
    let client = prepare_database!();

    #[derive(Debug, PartialEq, Serialize, Deserialize, Row)]
    struct MyRow {
        qb_small: QBit,
        qb_medium: QBit,
        qb_large: QBit,
    }

    client
        .query(
            r#"
            CREATE TABLE test_qbit_dimensions (
                qb_small    QBit(Float32, 64),
                qb_medium   QBit(Float32, 256),
                qb_large    QBit(Float32, 1024)
            ) ENGINE = MergeTree
            ORDER BY tuple()
            SETTINGS allow_experimental_qbit_type = 1;
            "#,
        )
        .execute()
        .await
        .unwrap();

    let row = MyRow {
        qb_small: QBit::from_data(vec![vec![5u8; 64]; 32]),
        qb_medium: QBit::from_data(vec![vec![6u8; 256]; 32]),
        qb_large: QBit::from_data(vec![vec![7u8; 1024]; 32]),
    };

    let mut insert = client.insert::<MyRow>("test_qbit_dimensions").await.unwrap();
    insert.write(&row).await.unwrap();
    insert.end().await.unwrap();

    let fetched_rows: Vec<MyRow> = client
        .query("SELECT ?fields FROM test_qbit_dimensions")
        .fetch_all()
        .await
        .unwrap();

    assert_eq!(fetched_rows.len(), 1);
    assert_eq!(fetched_rows[0], row);
}

#[tokio::test]
async fn qbit_nullable() {
    let client = prepare_database!();

    #[derive(Debug, PartialEq, Serialize, Deserialize, Row)]
    struct MyRow {
        qb_opt: Option<QBit>,
    }

    client
        .query(
            r#"
            CREATE TABLE test_qbit_nullable (
                qb_opt    Nullable(QBit(Float32, 128))
            ) ENGINE = MergeTree
            ORDER BY tuple()
            SETTINGS allow_experimental_qbit_type = 1;
            "#,
        )
        .execute()
        .await
        .unwrap();

    let row_with_value = MyRow {
        qb_opt: Some(QBit::from_data(vec![vec![8u8; 128]; 32])),
    };

    let row_with_null = MyRow {
        qb_opt: None,
    };

    let mut insert = client.insert::<MyRow>("test_qbit_nullable").await.unwrap();
    insert.write(&row_with_value).await.unwrap();
    insert.write(&row_with_null).await.unwrap();
    insert.end().await.unwrap();

    let fetched_rows: Vec<MyRow> = client
        .query("SELECT ?fields FROM test_qbit_nullable")
        .fetch_all()
        .await
        .unwrap();

    assert_eq!(fetched_rows.len(), 2);
    assert_eq!(fetched_rows[0], row_with_value);
    assert_eq!(fetched_rows[1], row_with_null);
}

#[tokio::test]
async fn qbit_low_cardinality() {
    let client = prepare_database!();

    #[derive(Debug, PartialEq, Serialize, Deserialize, Row)]
    struct MyRow {
        qb_lc: QBit,
    }

    client
        .query(
            r#"
            CREATE TABLE test_qbit_lc (
                qb_lc    LowCardinality(QBit(Float32, 128))
            ) ENGINE = MergeTree
            ORDER BY tuple()
            SETTINGS allow_experimental_qbit_type = 1;
            "#,
        )
        .execute()
        .await
        .unwrap();

    let row = MyRow {
        qb_lc: QBit::from_data(vec![vec![9u8; 128]; 32]),
    };

    let mut insert = client.insert::<MyRow>("test_qbit_lc").await.unwrap();
    insert.write(&row).await.unwrap();
    insert.end().await.unwrap();

    let fetched_rows: Vec<MyRow> = client
        .query("SELECT ?fields FROM test_qbit_lc")
        .fetch_all()
        .await
        .unwrap();

    assert_eq!(fetched_rows.len(), 1);
    assert_eq!(fetched_rows[0], row);
}
