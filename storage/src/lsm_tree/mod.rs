#[cfg(test)]
mod tests;

use crate::ss_table::SsTable;
use std::{
    collections::BTreeMap,
    error::Error,
    fs::File,
    hash::Hash,
    io::{BufReader, BufWriter, Write},
};

pub struct LsmTree<K, V>
where
    K: Hash + Clone + Ord + bincode::Encode + bincode::Decode<()>,
    V: Clone + bincode::Encode + bincode::Decode<()>,
{
    map: BTreeMap<K, Value<V>>,
    memtable_size: usize,
    data_directory: String,
    ss_table_block_size: usize,
    level_0_ss_tables: usize,
    level_1_ss_tables: usize,
    level_0_size: usize,
}

#[derive(bincode::Encode, bincode::Decode)]
struct State {
    ss_table_block_size: usize,
    memtable_size: usize,
    level_0_ss_tables: usize,
    level_1_ss_tables: usize,
    level_0_size: usize,
}

#[derive(bincode::Encode, bincode::Decode, Clone)]
enum Value<T>
where
    T: Clone,
{
    Data(T),
    Tombstone,
}

impl<K, V> Drop for LsmTree<K, V>
where
    K: Hash + Clone + Ord + bincode::Encode + bincode::Decode<()>,
    V: Clone + bincode::Encode + bincode::Decode<()>,
{
    fn drop(&mut self) {
        let _ = self.flush();
        // TODO: log this
    }
}

impl<K, V> LsmTree<K, V>
where
    K: Hash + Clone + Ord + bincode::Encode + bincode::Decode<()>,
    V: Clone + bincode::Encode + bincode::Decode<()>,
{
    pub fn new(
        data_directory: String,
        memtable_size: usize,
        level_0_size: usize,
        ss_table_block_size: usize,
    ) -> Result<Self, Box<dyn Error>> {
        assert!(
            !std::fs::exists(format!("{data_directory}/state"))?,
            "LSM tree already initialized at {data_directory}, use load() instead"
        );

        std::fs::create_dir_all(format!("{data_directory}/level0"))?;
        std::fs::create_dir_all(format!("{data_directory}/level1"))?;

        Ok(Self {
            map: BTreeMap::new(),
            memtable_size,
            data_directory,
            ss_table_block_size,
            level_0_ss_tables: 0,
            level_1_ss_tables: 0,
            level_0_size,
        })
    }

    pub fn load(data_directory: String) -> Result<Self, Box<dyn Error>> {
        let reader = BufReader::new(File::open(format!("{data_directory}/state"))?);

        let State {
            ss_table_block_size,
            memtable_size,
            level_0_ss_tables,
            level_1_ss_tables,
            level_0_size,
        }: State = bincode::decode_from_reader(reader, bincode::config::standard())?;

        Ok(Self {
            map: BTreeMap::new(),
            memtable_size,
            data_directory,
            ss_table_block_size,
            level_0_ss_tables,
            level_1_ss_tables,
            level_0_size,
        })
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<(), Box<dyn Error>> {
        if self.map.len() >= self.memtable_size {
            self.flush()?;
        }

        self.map.insert(key, Value::Data(value));
        Ok(())
    }

    pub fn get(&self, key: &K) -> Result<Option<V>, Box<dyn Error>> {
        if let Some(value) = self.map.get(key) {
            return match value {
                Value::Data(d) => Ok(Some(d.clone())),
                Value::Tombstone => Ok(None),
            };
        };

        for ss_table in (0..self.level_0_ss_tables).rev() {
            let path = format!("{}/level0/{ss_table}", self.data_directory);
            let ss_table = SsTable::<K, Value<V>>::load(path)?;

            if let Some(value) = ss_table.get(key)? {
                return match value {
                    Value::Data(d) => Ok(Some(d)),
                    Value::Tombstone => Ok(None),
                };
            }
        }

        for ss_table in (0..self.level_1_ss_tables).rev() {
            let path = format!("{}/level1/{ss_table}", self.data_directory);
            let ss_table = SsTable::<K, Value<V>>::load(path)?;

            if let Some(value) = ss_table.get(key)? {
                return match value {
                    Value::Data(d) => Ok(Some(d)),
                    Value::Tombstone => Ok(None),
                };
            }
        }

        Ok(None)
    }

    pub fn delete(&mut self, key: K) -> Result<Option<V>, Box<dyn Error>> {
        let value = self.get(&key)?;
        if value.is_none() {
            return Ok(None);
        };

        if self.map.len() >= self.memtable_size {
            self.flush()?;
        }

        self.map.insert(key, Value::Tombstone);
        Ok(value)
    }

    pub fn flush(&mut self) -> Result<(), Box<dyn Error>> {
        if self.map.is_empty() {
            return Ok(());
        }

        let mut map = BTreeMap::new();
        std::mem::swap(&mut self.map, &mut map);

        let path = format!("{}/level0/{}", self.data_directory, self.level_0_ss_tables);
        let _ = SsTable::new(map, &path, self.ss_table_block_size)?;

        self.level_0_ss_tables += 1;

        if self.level_0_ss_tables == self.level_0_size {
            self.compact()?;
        }

        self.save_state()?;

        Ok(())
    }

    pub fn compact(&mut self) -> Result<(), Box<dyn Error>> {
        let iters = (0..self.level_0_ss_tables)
            .map(|ss_table| {
                let path = format!("{}/level0/{ss_table}", self.data_directory);
                SsTable::<K, Value<V>>::load(path)?.iter()
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Actual compactions happen here, an implementation of FromIterator for BTreeMap takes
        // only the last (so most recent) item from the iterator.
        let map = iters
            .into_iter()
            .flatten()
            .collect::<Result<BTreeMap<K, Value<V>>, _>>()?;

        // TODO: We write all retained Tombstones here, without understanding if there were some
        //       values on level 1 before. Need to find a way to cleanup Tombstones without a
        //       corresponding values on deeper levels.

        let path = format!("{}/level1/{}", self.data_directory, self.level_1_ss_tables);
        let _ = SsTable::new(map, &path, self.ss_table_block_size)?;

        let level_0 = format!("{}/level0", self.data_directory);

        for file in std::fs::read_dir(level_0)? {
            let file = file?;
            let path = file.path();
            std::fs::remove_file(path)?;
        }

        self.level_1_ss_tables += 1;
        self.level_0_ss_tables = 0;

        self.save_state()?;

        Ok(())
    }

    fn save_state(&mut self) -> Result<(), Box<dyn Error>> {
        let mut writer = BufWriter::new(File::create(format!("{}/state", self.data_directory))?);

        let state = State {
            ss_table_block_size: self.ss_table_block_size,
            memtable_size: self.memtable_size,
            level_0_ss_tables: self.level_0_ss_tables,
            level_1_ss_tables: self.level_1_ss_tables,
            level_0_size: self.level_0_size,
        };

        let encoded_state = bincode::encode_to_vec(state, bincode::config::standard())?;

        writer.write_all(encoded_state.as_slice())?;
        writer.flush()?;
        Ok(())
    }
}
