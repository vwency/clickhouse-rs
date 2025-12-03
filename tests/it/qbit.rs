use serde::{Deserialize, Serialize};
use clickhouse::Row;
use clickhouse::serde::qbit::QBit;

#[tokio::test]
async fn qbit_roundtrip() {
    let client = prepare_database!();

    #[derive(Debug, PartialEq, Serialize, Deserialize, Row)]
    struct MyRow {
        qb16: QBit<128>,
        qb16_opt: Option<QBit<128>>,
        qb32: QBit<256>,
        qb32_opt: Option<QBit<256>>,
        qb64: QBit<512>,
        qb64_opt: Option<QBit<512>>,
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

    let qb16_data = QBit::<128>::from_data(vec![[1u8; 128]; 16]);
    let qb32_data = QBit::<256>::from_data(vec![[2u8; 256]; 32]);
    let qb64_data = QBit::<512>::from_data(vec![[3u8; 512]; 64]);

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
