use crate::{
    column::ColumnType,
    primary_key::{PrimaryKey, PrimaryKeyType},
    row::Row,
};
use dbcore::error::NornsDbError;
use journal::{Journal, JournalRecord, JournalRecordKind};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use storage::lsm_tree::LsmTree;

pub struct Table {
    schema: Arc<TableSchema>,
    storage: LsmTree<PrimaryKey, Row>,
    journal: Journal<PrimaryKey, Row>,
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct TableSchema {
    pub primary_key_name: String,
    pub primary_key_type: PrimaryKeyType,
    pub columns: Vec<(String, ColumnType)>,
}

#[derive(Debug, Copy, Clone)]
pub struct TableConfig {
    pub memtable_size: usize,
    pub level_0_size: usize,
    pub ss_table_block_size: usize,
}

impl Table {
    pub fn new(
        schema: TableSchema,
        data_directory: PathBuf,
        TableConfig {
            memtable_size,
            level_0_size,
            ss_table_block_size,
        }: TableConfig,
    ) -> Result<Self, NornsDbError> {
        std::fs::create_dir_all(&data_directory)?;

        let wal_path = Path::new(&data_directory).join("wal.log");
        let journal = Journal::new(wal_path)?;

        let storage = LsmTree::new(
            data_directory,
            memtable_size,
            level_0_size,
            ss_table_block_size,
        )?;

        Ok(Self {
            schema: Arc::new(schema),
            storage,
            journal,
        })
    }

    pub fn load(schema: TableSchema, data_directory: PathBuf) -> Result<Self, NornsDbError> {
        let wal_path = data_directory.join("wal.log");

        Ok(Self {
            schema: Arc::new(schema),
            storage: LsmTree::load(data_directory)?,
            journal: Journal::new(wal_path)?,
        })
    }

    pub async fn destroy(self) -> Result<(), NornsDbError> {
        self.journal.shutdown().await?;
        self.storage.destroy()?;
        Ok(())
    }

    pub async fn insert(&self, primary_key: PrimaryKey, row: Row) -> Result<(), NornsDbError> {
        self.validate_key(&primary_key)?;
        self.validate_row(&row)?;

        let record = JournalRecord::new(JournalRecordKind::Upsert {
            key: primary_key.clone(),
            value: row.clone(),
        });

        self.journal.append(&record).await?;

        self.storage.insert(primary_key, row)?;
        Ok(())
    }

    pub fn get(&self, primary_key: &PrimaryKey) -> Result<Option<Row>, NornsDbError> {
        self.storage.get(primary_key)
    }

    pub fn list(&self) -> Result<Vec<(PrimaryKey, Row)>, NornsDbError> {
        self.storage.list()
    }

    pub async fn delete(&self, primary_key: PrimaryKey) -> Result<Option<Row>, NornsDbError> {
        let record = JournalRecord::new(JournalRecordKind::Delete {
            key: primary_key.clone(),
        });

        self.journal.append(&record).await?;

        self.storage.delete(primary_key)
    }

    pub fn schema(&self) -> Arc<TableSchema> {
        self.schema.clone()
    }

    fn validate_key(&self, key: &PrimaryKey) -> Result<(), NornsDbError> {
        if !key.is_a(self.schema.primary_key_type) {
            return Err(NornsDbError::PrimaryKeyTypeMismatch {
                expected: format!("{:?}", self.schema.primary_key_type),
                actual: format!("{:?}", key.pk_type()),
            });
        }

        Ok(())
    }

    fn validate_row(&self, row: &Row) -> Result<(), NornsDbError> {
        if row.0.len() != self.schema.columns.len() {
            return Err(NornsDbError::ColumnCountMismatch {
                expected: self.schema.columns.len(),
                actual: row.0.len(),
            });
        }

        for (i, (col, (name, expected_type))) in
            row.0.iter().zip(self.schema.columns.iter()).enumerate()
        {
            if !col.is_a(*expected_type) {
                return Err(NornsDbError::ColumnTypeMismatch {
                    index: i,
                    name: name.clone(),
                    expected: format!("{expected_type:?}"),
                    actual: format!("{:?}", col.col_type()),
                });
            }
        }

        Ok(())
    }
}
