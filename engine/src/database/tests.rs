use super::*;
use crate::{
    column::{Column, ColumnType},
    primary_key::PrimaryKeyType,
};

fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join("norns_db_database_tests")
        .join(name);

    if dir.exists() {
        std::fs::remove_dir_all(&dir).unwrap();
    }
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn test_config() -> DatabaseConfig {
    DatabaseConfig {
        memtable_size: 64,
        level_0_size: 4,
        ss_table_block_size: 256,
    }
}

fn users_schema() -> TableSchema {
    TableSchema {
        primary_key_name: "id".to_string(),
        primary_key_type: PrimaryKeyType::Integer,
        columns: vec![("name".to_string(), ColumnType::Varchar)],
    }
}

#[tokio::test]
async fn test_create_table() {
    let db = Database::new(test_dir("test_create_table"), test_config()).unwrap();

    db.create_table("users", users_schema()).await.unwrap();

    let names = db.table_names().await;
    assert!(names.contains(&"users".to_string()));
}

#[tokio::test]
async fn test_create_table_when_name_already_exists() {
    let db = Database::new(
        test_dir("test_create_table_when_name_already_exists"),
        test_config(),
    )
    .unwrap();

    db.create_table("users", users_schema()).await.unwrap();

    let result = db.create_table("users", users_schema()).await;
    assert!(matches!(
        result,
        Err(NornsDbError::TableAlreadyExists { .. })
    ));
}

#[tokio::test]
async fn test_insert() {
    let db = Database::new(test_dir("test_insert"), test_config()).unwrap();
    db.create_table("users", users_schema()).await.unwrap();

    let key = PrimaryKey::Integer(1);
    let row = Row(vec![Column::Varchar("Alice".to_string())]);
    db.insert("users", key.clone(), row).await.unwrap();

    let result = db.get("users", &key).await.unwrap();
    assert!(matches!(
        result,
        Some(Row(cols)) if matches!(cols.as_slice(), [Column::Varchar(name)] if name == "Alice")
    ));
}

#[tokio::test]
async fn test_insert_when_table_not_found() {
    let db = Database::new(test_dir("test_insert_when_table_not_found"), test_config()).unwrap();

    let key = PrimaryKey::Integer(1);
    let row = Row(vec![Column::Varchar("Alice".to_string())]);

    let result = db.insert("missing", key, row).await;
    assert!(matches!(result, Err(NornsDbError::TableNotFound { .. })));
}

#[tokio::test]
async fn test_insert_with_wrong_key_type() {
    let db = Database::new(test_dir("test_insert_with_wrong_key_type"), test_config()).unwrap();
    db.create_table("users", users_schema()).await.unwrap();

    let key = PrimaryKey::Varchar("not_an_integer".to_string());
    let row = Row(vec![Column::Varchar("Alice".to_string())]);

    let result = db.insert("users", key, row).await;
    assert!(matches!(
        result,
        Err(NornsDbError::PrimaryKeyTypeMismatch { .. })
    ));
}

#[tokio::test]
async fn test_insert_with_wrong_column_count() {
    let db = Database::new(
        test_dir("test_insert_with_wrong_column_count"),
        test_config(),
    )
    .unwrap();
    db.create_table("users", users_schema()).await.unwrap();

    let key = PrimaryKey::Integer(1);
    let row = Row(vec![
        Column::Varchar("Alice".to_string()),
        Column::Integer(42),
    ]);

    let result = db.insert("users", key, row).await;
    assert!(matches!(
        result,
        Err(NornsDbError::ColumnCountMismatch { .. })
    ));
}

#[tokio::test]
async fn test_insert_with_wrong_column_type() {
    let db = Database::new(
        test_dir("test_insert_with_wrong_column_type"),
        test_config(),
    )
    .unwrap();
    db.create_table("users", users_schema()).await.unwrap();

    let key = PrimaryKey::Integer(1);
    let row = Row(vec![Column::Integer(42)]);

    let result = db.insert("users", key, row).await;
    assert!(matches!(
        result,
        Err(NornsDbError::ColumnTypeMismatch { .. })
    ));
}

#[tokio::test]
async fn test_get() {
    let db = Database::new(test_dir("test_get"), test_config()).unwrap();
    db.create_table("users", users_schema()).await.unwrap();

    let key = PrimaryKey::Integer(1);
    let row = Row(vec![Column::Varchar("Alice".to_string())]);
    db.insert("users", key.clone(), row).await.unwrap();

    let result = db.get("users", &key).await.unwrap();
    assert!(matches!(
        result,
        Some(Row(cols)) if matches!(cols.as_slice(), [Column::Varchar(name)] if name == "Alice")
    ));
}

#[tokio::test]
async fn test_get_with_missing_key() {
    let db = Database::new(test_dir("test_get_with_missing_key"), test_config()).unwrap();
    db.create_table("users", users_schema()).await.unwrap();

    let result = db.get("users", &PrimaryKey::Integer(999)).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_get_when_table_not_found() {
    let db = Database::new(test_dir("test_get_when_table_not_found"), test_config()).unwrap();

    let result = db.get("missing", &PrimaryKey::Integer(1)).await;
    assert!(matches!(result, Err(NornsDbError::TableNotFound { .. })));
}

#[tokio::test]
async fn test_delete() {
    let db = Database::new(test_dir("test_delete"), test_config()).unwrap();
    db.create_table("users", users_schema()).await.unwrap();

    let key = PrimaryKey::Integer(1);
    let row = Row(vec![Column::Varchar("Alice".to_string())]);
    db.insert("users", key.clone(), row).await.unwrap();

    db.delete("users", key.clone()).await.unwrap();

    let result = db.get("users", &key).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_delete_with_missing_key() {
    let db = Database::new(test_dir("test_delete_with_missing_key"), test_config()).unwrap();
    db.create_table("users", users_schema()).await.unwrap();

    let result = db.delete("users", PrimaryKey::Integer(999)).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_delete_when_table_not_found() {
    let db = Database::new(test_dir("test_delete_when_table_not_found"), test_config()).unwrap();

    let result = db.delete("missing", PrimaryKey::Integer(1)).await;
    assert!(matches!(result, Err(NornsDbError::TableNotFound { .. })));
}

#[tokio::test]
async fn test_drop_table() {
    let db = Database::new(test_dir("test_drop_table"), test_config()).unwrap();

    db.create_table("users", users_schema()).await.unwrap();
    db.drop_table("users").await.unwrap();

    let result = db.get("users", &PrimaryKey::Integer(1)).await;
    assert!(matches!(result, Err(NornsDbError::TableNotFound { .. })));
}

#[tokio::test]
async fn test_drop_table_when_table_not_found() {
    let db = Database::new(
        test_dir("test_drop_table_when_table_not_found"),
        test_config(),
    )
    .unwrap();

    let result = db.drop_table("missing").await;
    assert!(matches!(result, Err(NornsDbError::TableNotFound { .. })));
}

#[tokio::test]
async fn test_load() {
    let db_path = test_dir("test_load");

    {
        let db = Database::new(&db_path, test_config()).unwrap();
        db.create_table("users", users_schema()).await.unwrap();

        let key = PrimaryKey::Integer(42);
        let row = Row(vec![Column::Varchar("Bob".to_string())]);
        db.insert("users", key, row).await.unwrap();
    }

    let db = Database::load(&db_path, test_config()).unwrap();

    let result = db.get("users", &PrimaryKey::Integer(42)).await.unwrap();
    assert!(matches!(
        result,
        Some(Row(cols)) if matches!(cols.as_slice(), [Column::Varchar(name)] if name == "Bob")
    ));
}

#[tokio::test]
async fn test_load_with_created_table() {
    let db_path = test_dir("test_load_with_created_table");

    {
        let db = Database::new(&db_path, test_config()).unwrap();
        db.create_table("users", users_schema()).await.unwrap();
    }

    let db = Database::load(&db_path, test_config()).unwrap();

    let names = db.table_names().await;
    assert!(names.contains(&"users".to_string()));
}

#[tokio::test]
async fn test_load_with_multiple_tables() {
    let db_path = test_dir("test_load_with_multiple_tables");

    let orders_schema = TableSchema {
        primary_key_name: "order_id".to_string(),
        primary_key_type: PrimaryKeyType::BigInteger,
        columns: vec![
            ("product".to_string(), ColumnType::Varchar),
            ("quantity".to_string(), ColumnType::Integer),
        ],
    };

    {
        let db = Database::new(&db_path, test_config()).unwrap();

        db.create_table("users", users_schema()).await.unwrap();
        db.create_table("orders", orders_schema).await.unwrap();
    }

    let db = Database::load(&db_path, test_config()).unwrap();

    let mut names = db.table_names().await;
    names.sort();
    assert_eq!(names, vec!["orders", "users"]);
}

#[tokio::test]
async fn test_load_when_table_was_dropped() {
    let db_path = test_dir("test_load_when_table_was_dropped");

    {
        let db = Database::new(&db_path, test_config()).unwrap();

        db.create_table("users", users_schema()).await.unwrap();
        db.create_table("orders", users_schema()).await.unwrap();

        db.drop_table("orders").await.unwrap();
    }

    let db = Database::load(&db_path, test_config()).unwrap();

    let names = db.table_names().await;
    assert_eq!(names, vec!["users"]);
}

#[tokio::test]
async fn test_load_when_no_tables_exist() {
    let db_path = test_dir("test_load_no_tables");

    {
        let _db = Database::new(&db_path, test_config()).unwrap();
    }

    let db = Database::load(&db_path, test_config()).unwrap();

    let names = db.table_names().await;
    assert!(names.is_empty());
}

#[tokio::test]
async fn test_load_with_data_preserved_across_restart() {
    let db_path = test_dir("test_load_data_preserved");

    {
        let db = Database::new(&db_path, test_config()).unwrap();
        db.create_table("users", users_schema()).await.unwrap();

        let key = PrimaryKey::Integer(1);
        let row = Row(vec![Column::Varchar("Alice".to_string())]);
        db.insert("users", key, row).await.unwrap();

        let key = PrimaryKey::Integer(2);
        let row = Row(vec![Column::Varchar("Bob".to_string())]);
        db.insert("users", key, row).await.unwrap();
    }

    let db = Database::load(&db_path, test_config()).unwrap();

    let result = db.get("users", &PrimaryKey::Integer(1)).await.unwrap();
    assert!(matches!(
        result,
        Some(Row(cols)) if matches!(cols.as_slice(), [Column::Varchar(name)] if name == "Alice")
    ));

    let result = db.get("users", &PrimaryKey::Integer(2)).await.unwrap();
    assert!(matches!(
        result,
        Some(Row(cols)) if matches!(cols.as_slice(), [Column::Varchar(name)] if name == "Bob")
    ));
}

#[tokio::test]
async fn test_destroy() {
    let db_path = test_dir("test_destroy");

    let db = Database::new(&db_path, test_config()).unwrap();
    db.create_table("users", users_schema()).await.unwrap();

    db.destroy().await.unwrap();
    assert!(!db_path.exists());
}

#[tokio::test]
async fn test_table_names() {
    let db = Database::new(test_dir("test_table_names"), test_config()).unwrap();

    db.create_table("users", users_schema()).await.unwrap();
    db.create_table("orders", users_schema()).await.unwrap();

    let mut names = db.table_names().await;
    names.sort();
    assert_eq!(names, vec!["orders", "users"]);
}

#[tokio::test]
async fn test_insert_and_get_with_overwritten_key() {
    let db = Database::new(
        test_dir("test_insert_and_get_with_overwritten_key"),
        test_config(),
    )
    .unwrap();
    db.create_table("users", users_schema()).await.unwrap();

    let key = PrimaryKey::Integer(1);

    let row = Row(vec![Column::Varchar("Alice".to_string())]);
    db.insert("users", key.clone(), row).await.unwrap();

    let row = Row(vec![Column::Varchar("Bob".to_string())]);
    db.insert("users", key.clone(), row).await.unwrap();

    let result = db.get("users", &key).await.unwrap();
    assert!(matches!(
        result,
        Some(Row(cols)) if matches!(cols.as_slice(), [Column::Varchar(name)] if name == "Bob")
    ));
}
