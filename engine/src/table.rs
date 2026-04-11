use crate::{
    column::ColumnType,
    primary_key::{PrimaryKey, PrimaryKeyType},
    row::Row,
};
use dbcore::error::NornsDbError;
use journal::{Journal, JournalHandle, JournalRecordData};
use std::{path::PathBuf, sync::Arc};
use storage::lsm_tree::LsmTree;
use tokio::sync::Mutex;
use tracing::error;

pub struct Table {
    schema: Arc<TableSchema>,
    storage: LsmTree<PrimaryKey, Row, JournalHandle>,
    journal: Arc<Mutex<Journal<PrimaryKey, Row>>>,
    _commit_task: tokio::task::JoinHandle<()>,
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
    pub max_frozen_memtables: usize,
}

impl Table {
    pub fn new(
        name: String,
        schema: TableSchema,
        data_directory: PathBuf,
        TableConfig {
            memtable_size,
            level_0_size,
            ss_table_block_size,
            max_frozen_memtables,
        }: TableConfig,
    ) -> Result<Self, NornsDbError> {
        std::fs::create_dir_all(&data_directory)?;

        let wal_path = data_directory.join("wal.log");
        let journal = Arc::new(Mutex::new(Journal::new(wal_path)?));

        let (commit_tx, mut commit_rx) = tokio::sync::mpsc::unbounded_channel::<JournalHandle>();

        let journal_ref = journal.clone();

        let commit_task = tokio::spawn(async move {
            while let Some(handle) = commit_rx.recv().await {
                let j = journal_ref.lock().await;
                if let Err(e) = j.commit(handle).await {
                    error!(?e, "journal commit failed after flush");
                }
            }
        });

        let storage = LsmTree::new(
            name,
            data_directory,
            memtable_size,
            level_0_size,
            ss_table_block_size,
            max_frozen_memtables,
            commit_tx,
        )?;

        Ok(Self {
            schema: Arc::new(schema),
            storage,
            journal,
            _commit_task: commit_task,
        })
    }

    pub fn load(
        name: String,
        schema: TableSchema,
        data_directory: PathBuf,
        max_frozen_memtables: usize,
    ) -> Result<Self, NornsDbError> {
        let wal_path = data_directory.join("wal.log");
        let journal = Arc::new(Mutex::new(Journal::new(wal_path)?));

        let (commit_tx, mut commit_rx) = tokio::sync::mpsc::unbounded_channel::<JournalHandle>();

        let journal_ref = journal.clone();
        let commit_task = tokio::spawn(async move {
            while let Some(handle) = commit_rx.recv().await {
                let j = journal_ref.lock().await;
                if let Err(e) = j.commit(handle).await {
                    error!(?e, "journal commit failed after flush");
                }
            }
        });

        Ok(Self {
            schema: Arc::new(schema),
            storage: LsmTree::load(name, data_directory, max_frozen_memtables, commit_tx)?,
            journal,
            _commit_task: commit_task,
        })
    }

    pub async fn destroy(self) -> Result<(), NornsDbError> {
        let Self {
            storage,
            journal,
            _commit_task,
            ..
        } = self;

        storage.destroy()?;
        _commit_task.await.ok();

        let journal = Arc::try_unwrap(journal)
            .map_err(|_| NornsDbError::impossible("journal Arc has unexpected extra references"))?
            .into_inner();

        journal.shutdown().await
    }

    pub async fn insert(&self, primary_key: PrimaryKey, row: Row) -> Result<(), NornsDbError> {
        self.validate_key(&primary_key)?;
        self.validate_row(&row)?;

        let record = JournalRecordData::Upsert {
            key: primary_key.clone(),
            value: row.clone(),
        };

        // Hold the journal lock across append + storage insert to preserve ordering.
        {
            let journal = self.journal.lock().await;
            let handle = journal.append(record).await?;
            self.storage.insert(primary_key, row, handle)
        }
    }

    pub fn get(&self, primary_key: &PrimaryKey) -> Result<Option<Row>, NornsDbError> {
        self.storage.get(primary_key)
    }

    pub fn list(&self) -> Result<Vec<(PrimaryKey, Row)>, NornsDbError> {
        self.storage.list()
    }

    pub async fn delete(&self, primary_key: PrimaryKey) -> Result<(), NornsDbError> {
        let record = JournalRecordData::Delete {
            key: primary_key.clone(),
        };

        // Hold the journal lock across append + storage delete to preserve ordering.
        {
            let journal = self.journal.lock().await;
            let handle = journal.append(record).await?;
            self.storage.delete(primary_key, handle)
        }
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
