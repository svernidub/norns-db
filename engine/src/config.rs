use crate::DatabaseConfig;
use dbcore::error::NornsDbError;
use envconfig::Envconfig;
use std::path::PathBuf;

#[derive(Debug, Clone, Envconfig)]
pub struct NornsConfig {
    #[envconfig(from = "NORNS_DATA_DIR", default = "data")]
    data_directory: String,

    #[envconfig(from = "NORNS_MEMTABLE_SIZE", default = "10000")]
    memtable_size: usize,

    #[envconfig(from = "NORNS_LEVEL0_SIZE", default = "15")]
    level_0_size: usize,

    #[envconfig(from = "NORNS_SSTABLE_BLOCK_SIZE", default = "4096")]
    ss_table_block_size: usize,

    #[envconfig(from = "NORNS_MAX_FROZEN_MEMTABLES", default = "2")]
    max_frozen_memtables: usize,
}

impl NornsConfig {
    pub fn validate(&self) -> Result<(), NornsDbError> {
        if self.data_directory.is_empty() {
            return Err(NornsDbError::Config(
                "NORNS_DATA_DIR must not be empty".to_string(),
            ));
        }

        if self.memtable_size == 0 {
            return Err(NornsDbError::Config(
                "NORNS_MEMTABLE_SIZE must be > 0".to_string(),
            ));
        }

        if self.level_0_size == 0 {
            return Err(NornsDbError::Config(
                "NORNS_LEVEL0_SIZE must be > 0".to_string(),
            ));
        }

        if self.ss_table_block_size == 0 {
            return Err(NornsDbError::Config(
                "NORNS_SSTABLE_BLOCK_SIZE must be > 0".to_string(),
            ));
        }

        if self.max_frozen_memtables == 0 {
            return Err(NornsDbError::Config(
                "NORNS_MAX_FROZEN_MEMTABLES must be > 0".to_string(),
            ));
        }

        Ok(())
    }

    pub fn load_from_env() -> Result<Self, NornsDbError> {
        let cfg = NornsConfig::init_from_env()
            .map_err(|e| NornsDbError::Config(format!("failed to load config from env: {e}")))?;

        cfg.validate()?;
        Ok(cfg)
    }

    pub fn data_directory_path(&self) -> PathBuf {
        PathBuf::from(self.data_directory.as_str())
    }

    pub fn database_config(&self) -> DatabaseConfig {
        DatabaseConfig {
            memtable_size: self.memtable_size,
            level_0_size: self.level_0_size,
            ss_table_block_size: self.ss_table_block_size,
            max_frozen_memtables: self.max_frozen_memtables,
        }
    }

    pub fn memtable_size(&self) -> usize {
        self.memtable_size
    }

    pub fn level_0_size(&self) -> usize {
        self.level_0_size
    }

    pub fn ss_table_block_size(&self) -> usize {
        self.ss_table_block_size
    }

    pub fn max_frozen_memtables(&self) -> usize {
        self.max_frozen_memtables
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_from_env_defaults() {
        let empty = HashMap::new();
        let cfg = NornsConfig::init_from_hashmap(&empty).unwrap();
        assert_eq!(cfg.data_directory_path(), PathBuf::from("data"));
        assert_eq!(cfg.memtable_size(), 10_000);
        assert_eq!(cfg.level_0_size(), 15);
        assert_eq!(cfg.ss_table_block_size(), 4096);
    }

    #[test]
    fn test_from_env_overrides() {
        let mut vars = HashMap::new();
        vars.insert("NORNS_DATA_DIR".to_string(), "custom_data".to_string());
        vars.insert("NORNS_MEMTABLE_SIZE".to_string(), "123".to_string());
        vars.insert("NORNS_LEVEL0_SIZE".to_string(), "7".to_string());
        vars.insert("NORNS_SSTABLE_BLOCK_SIZE".to_string(), "2048".to_string());

        let cfg = NornsConfig::init_from_hashmap(&vars).unwrap();
        cfg.validate().unwrap();

        assert_eq!(cfg.data_directory_path(), PathBuf::from("custom_data"));
        assert_eq!(cfg.memtable_size(), 123);
        assert_eq!(cfg.level_0_size(), 7);
        assert_eq!(cfg.ss_table_block_size(), 2048);
    }
}
