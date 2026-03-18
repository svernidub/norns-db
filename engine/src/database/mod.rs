#[cfg(test)]
mod tests;

use crate::{
    primary_key::PrimaryKey,
    row::Row,
    table::{Table, TableConfig, TableSchema},
};
use dbcore::error::NornsDbError;
use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::RwLock;

pub struct Database {
    // RwLock here may look a weird choice, but for now it should be fine. Both readers or writes
    // will take a read() locks and the "DDL" actions will take a write() lock.
    //
    // For now it kinda emulates "table level locks" (or rather db-level locks). Maybe will
    // consider a HashMap of RwLocks or some other mechanism in the future.
    tables: RwLock<HashMap<String, Table>>,

    data_directory: PathBuf,
    config: DatabaseConfig,
}

#[derive(bincode::Encode, bincode::Decode)]
pub struct DatabaseSchema {
    pub tables: HashMap<String, TableSchema>,
}

#[derive(Debug, Copy, Clone)]
pub struct DatabaseConfig {
    pub memtable_size: usize,
    pub level_0_size: usize,
    pub ss_table_block_size: usize,
}

impl From<DatabaseConfig> for TableConfig {
    fn from(config: DatabaseConfig) -> Self {
        let DatabaseConfig {
            memtable_size,
            level_0_size,
            ss_table_block_size,
        } = config;

        Self {
            memtable_size,
            level_0_size,
            ss_table_block_size,
        }
    }
}

impl Database {
    pub fn new(
        data_directory: impl Into<PathBuf>,
        config: DatabaseConfig,
    ) -> Result<Self, NornsDbError> {
        let data_directory = data_directory.into();
        std::fs::create_dir_all(&data_directory)?;

        let db = Self {
            tables: RwLock::new(HashMap::new()),
            data_directory,
            config,
        };
        db.save_schema(HashMap::new())?;

        Ok(db)
    }

    pub fn load(
        data_directory: impl Into<PathBuf>,
        config: DatabaseConfig,
    ) -> Result<Self, NornsDbError> {
        let data_directory = data_directory.into();
        let schema = Self::load_schema(&data_directory)?;
        let mut tables = HashMap::new();

        for (name, table_schema) in schema.tables {
            let table_dir = data_directory.join(&name);
            let table = Table::load(table_schema, table_dir)?;
            tables.insert(name, table);
        }

        Ok(Self {
            tables: RwLock::new(tables),
            data_directory,
            config,
        })
    }

    pub async fn destroy(self) -> Result<(), NornsDbError> {
        for (_, table) in self.tables.into_inner() {
            table.destroy().await?;
        }
        std::fs::remove_dir_all(&self.data_directory)?;
        Ok(())
    }

    pub async fn create_table(
        &self,
        name: impl Into<String>,
        schema: TableSchema,
    ) -> Result<(), NornsDbError> {
        let name = name.into();

        let mut tables = self.tables.write().await;
        if tables.contains_key(&name) {
            return Err(NornsDbError::TableAlreadyExists { name });
        }

        let table_dir = self.data_directory.join(&name);
        let table = Table::new(schema, table_dir, self.config.into())?;
        tables.insert(name, table);
        self.extract_schema_from_tables(&tables)?;
        Ok(())
    }

    pub async fn drop_table(&self, name: impl Into<String>) -> Result<(), NornsDbError> {
        let name = name.into();
        let mut tables = self.tables.write().await;

        let table = tables
            .remove(&name)
            .ok_or_else(|| NornsDbError::TableNotFound { name: name.clone() })?;

        table.destroy().await?;
        self.extract_schema_from_tables(&tables)?;
        Ok(())
    }

    pub async fn insert(&self, table: &str, key: PrimaryKey, row: Row) -> Result<(), NornsDbError> {
        let tables = self.tables.read().await;
        let table = tables
            .get(table)
            .ok_or_else(|| NornsDbError::TableNotFound {
                name: table.to_string(),
            })?;
        table.insert(key, row).await
    }

    pub async fn get(&self, table: &str, key: &PrimaryKey) -> Result<Option<Row>, NornsDbError> {
        let tables = self.tables.read().await;
        let table = tables
            .get(table)
            .ok_or_else(|| NornsDbError::TableNotFound {
                name: table.to_string(),
            })?;
        table.get(key)
    }

    pub async fn delete(&self, table: &str, key: PrimaryKey) -> Result<Option<Row>, NornsDbError> {
        let tables = self.tables.read().await;
        let table = tables
            .get(table)
            .ok_or_else(|| NornsDbError::TableNotFound {
                name: table.to_string(),
            })?;
        table.delete(key).await
    }

    pub async fn list_rows(&self, table: &str) -> Result<Vec<(PrimaryKey, Row)>, NornsDbError> {
        let tables = self.tables.read().await;
        let table = tables
            .get(table)
            .ok_or_else(|| NornsDbError::TableNotFound {
                name: table.to_string(),
            })?;

        table.list()
    }

    pub async fn table_names(&self) -> Vec<String> {
        let tables = self.tables.read().await;
        tables.keys().cloned().collect()
    }

    pub async fn table_schema(&self, name: &str) -> Option<Arc<TableSchema>> {
        let tables = self.tables.read().await;
        tables.get(name).map(|table| table.schema().clone())
    }

    fn extract_schema_from_tables(
        &self,
        tables: &HashMap<String, Table>,
    ) -> Result<(), NornsDbError> {
        let schema_map: HashMap<String, _> = tables
            .iter()
            .map(|(name, table)| (name.clone(), table.schema().as_ref().clone()))
            .collect();

        self.save_schema(schema_map)
    }

    fn save_schema(&self, tables: HashMap<String, TableSchema>) -> Result<(), NornsDbError> {
        let schema = DatabaseSchema { tables };
        let encoded = bincode::encode_to_vec(schema, bincode::config::standard())?;

        let tmp_path = self.data_directory.join("schema.tmp");
        let tmp_file = File::create(self.data_directory.join("schema.tmp"))?;

        let mut writer = BufWriter::new(&tmp_file);
        writer.write_all(&encoded)?;
        writer.flush()?;
        tmp_file.sync_all()?;

        let final_path = self.data_directory.join("schema");
        std::fs::rename(&tmp_path, &final_path)?;

        Ok(())
    }

    fn load_schema(data_directory: &Path) -> Result<DatabaseSchema, NornsDbError> {
        let reader = BufReader::new(File::open(data_directory.join("schema"))?);
        let schema: DatabaseSchema =
            bincode::decode_from_reader(reader, bincode::config::standard())?;

        Ok(schema)
    }
}
