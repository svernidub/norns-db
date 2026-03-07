#[cfg(test)]
mod tests;

use crate::ss_table::SsTable;
use arc_swap::{ArcSwap, ArcSwapOption};
use metrics::{counter, gauge, histogram};
use std::{
    collections::BTreeMap,
    error::Error,
    fs::File,
    hash::Hash,
    io::{BufReader, BufWriter, Write},
    path::Path,
    sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard},
    time::Instant,
};
use tracing::{debug, info, trace};

pub struct LsmTree<K, V>
where
    K: Hash + Clone + Ord + bincode::Encode + bincode::Decode<()>,
    V: Clone + bincode::Encode + bincode::Decode<()>,
{
    memtable: RwLock<BTreeMap<K, Value<V>>>,
    memtable_size: usize,
    data_directory: String,
    ss_table_block_size: usize,
    level_0_size: usize,

    // A place where we dump the main memtable when it's full. A buffer for flushing on
    // disk, accessible for readers.
    frozen_memtable: ArcSwapOption<BTreeMap<K, Value<V>>>,

    // Data is dumped on disc with CoW principle, so all readers are not blocked.
    // But concurrent writes may be blocked during a flush & compaction.
    store_lock: Mutex<()>,
    current_store_version: ArcSwap<Storage<K, V>>,
}

struct Storage<K, V>
where
    V: Clone,
{
    level_0_ss_tables: Vec<Arc<SsTable<K, Value<V>>>>,
    level_1_ss_tables: Vec<Arc<SsTable<K, Value<V>>>>,
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
        debug!(dir = %self.data_directory, "LSM tree shutting down, flushing memtable");
        if let Err(e) = self.flush() {
            debug!(error = %e, "failed to flush memtable on drop");
        }
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
            !std::fs::exists(Path::new(&data_directory).join("state"))?,
            "LSM tree already initialized at {data_directory}, use load() instead"
        );

        std::fs::create_dir_all(Path::new(&data_directory).join("level0"))?;
        std::fs::create_dir_all(Path::new(&data_directory).join("level1"))?;

        debug!(
            dir = %data_directory,
            memtable_size,
            level_0_size,
            ss_table_block_size,
            "LSM tree created"
        );

        gauge!("lsm.memtable_entries").set(0.0);
        gauge!("lsm.l0_tables").set(0.0);
        gauge!("lsm.l1_tables").set(0.0);

        Ok(Self {
            memtable: RwLock::new(BTreeMap::new()),
            memtable_size,
            data_directory,
            ss_table_block_size,
            current_store_version: ArcSwap::from_pointee(Storage {
                level_0_ss_tables: vec![],
                level_1_ss_tables: vec![],
            }),
            level_0_size,
            frozen_memtable: ArcSwapOption::empty(),
            store_lock: Mutex::new(()),
        })
    }

    pub fn load(data_directory: String) -> Result<Self, Box<dyn Error>> {
        debug!(dir = %data_directory, "loading LSM tree");

        let reader = BufReader::new(File::open(Path::new(&data_directory).join("state"))?);

        let State {
            ss_table_block_size,
            memtable_size,
            level_0_ss_tables,
            level_1_ss_tables,
            level_0_size,
        }: State = bincode::decode_from_reader(reader, bincode::config::standard())?;

        debug!(
            dir = %data_directory,
            memtable_size,
            level_0_size,
            ss_table_block_size,
            l0_tables = level_0_ss_tables,
            l1_tables = level_1_ss_tables,
            "LSM tree state loaded"
        );

        let level_0_ss_tables = Self::load_level(&data_directory, "level0", level_0_ss_tables)?;
        let level_1_ss_tables = Self::load_level(&data_directory, "level1", level_1_ss_tables)?;

        gauge!("lsm.memtable_entries").set(0.0);
        gauge!("lsm.l0_tables").set(level_0_ss_tables.len() as f64);
        gauge!("lsm.l1_tables").set(level_1_ss_tables.len() as f64);

        Ok(Self {
            memtable: RwLock::new(BTreeMap::new()),
            memtable_size,
            data_directory,
            ss_table_block_size,
            level_0_size,
            frozen_memtable: ArcSwapOption::empty(),
            store_lock: Mutex::new(()),
            current_store_version: ArcSwap::from_pointee(Storage {
                level_0_ss_tables,
                level_1_ss_tables,
            }),
        })
    }

    // FIXME: deal with this later
    #[allow(clippy::type_complexity)]
    fn load_level(
        data_directory: &str,
        level_name: &str,
        tables_number: usize,
    ) -> Result<Vec<Arc<SsTable<K, Value<V>>>>, Box<dyn Error>> {
        let mut level_ss_tables = Vec::with_capacity(tables_number);

        for ss_table in 0..tables_number {
            let path = Path::new(data_directory)
                .join(level_name)
                .join(ss_table.to_string());

            let table = Arc::new(SsTable::<K, Value<V>>::load(path)?);
            level_ss_tables.push(table);
        }
        Ok(level_ss_tables)
    }

    pub fn insert(&self, key: K, value: V) -> Result<(), Box<dyn Error>> {
        let needs_flush = {
            let memtable = lock_read(&self.memtable);
            memtable.len() >= self.memtable_size
        };

        if needs_flush {
            debug!("memtable full, triggering flush before insert");
            self.flush()?;
        }

        trace!("inserting key into memtable");
        let size = {
            let mut memtable = lock_write(&self.memtable);
            memtable.insert(key, Value::Data(value));
            memtable.len()
        };

        counter!("lsm.inserts").increment(1);
        gauge!("lsm.memtable_entries").set(size as f64);
        Ok(())
    }

    pub fn get(&self, key: &K) -> Result<Option<V>, Box<dyn Error>> {
        counter!("lsm.gets").increment(1);

        {
            let memtable = lock_read(&self.memtable);
            if let Some(value) = memtable.get(key) {
                trace!("key found in active memtable");
                return match value {
                    Value::Data(d) => Ok(Some(d.clone())),
                    Value::Tombstone => Ok(None),
                };
            }
        }

        {
            let frozen_memtable = self.frozen_memtable.load();

            let maybe_value = frozen_memtable
                .as_ref()
                .and_then(|frozen_mt| frozen_mt.get(key).cloned());

            if let Some(value) = maybe_value {
                trace!("key found in frozen memtable");
                return match value {
                    Value::Data(d) => Ok(Some(d)),
                    Value::Tombstone => Ok(None),
                };
            }
        }

        let current_store_version = self.current_store_version.load();

        if let Some(value) = self.lookup_in_level(&current_store_version.level_0_ss_tables, key)? {
            trace!("key found in L0 SSTables");
            return Ok(Some(value));
        }

        if let Some(value) = self.lookup_in_level(&current_store_version.level_1_ss_tables, key)? {
            trace!("key found in L1 SSTables");
            return Ok(Some(value));
        }

        trace!("key not found in any level");
        Ok(None)
    }

    fn lookup_in_level(
        &self,
        level: &[Arc<SsTable<K, Value<V>>>],
        key: &K,
    ) -> Result<Option<V>, Box<dyn Error>> {
        for ss_table in level.iter().rev() {
            if let Some(value) = ss_table.get(key)? {
                return match value {
                    Value::Data(d) => Ok(Some(d)),
                    Value::Tombstone => Ok(None),
                };
            }
        }

        Ok(None)
    }

    pub fn delete(&self, key: K) -> Result<Option<V>, Box<dyn Error>> {
        let value = self.get(&key)?;
        if value.is_none() {
            debug!("delete: key not found, skipping");
            return Ok(None);
        }

        let needs_flush = {
            let memtable = lock_read(&self.memtable);
            memtable.len() >= self.memtable_size
        };

        if needs_flush {
            debug!("memtable full, triggering flush before delete");
            self.flush()?;
        }

        debug!("writing tombstone for key");
        let size = {
            let mut memtable = lock_write(&self.memtable);
            memtable.insert(key, Value::Tombstone);
            memtable.len()
        };

        counter!("lsm.deletes").increment(1);
        gauge!("lsm.memtable_entries").set(size as f64);
        Ok(value)
    }

    pub fn flush(&self) -> Result<(), Box<dyn Error>> {
        let _guard = self.store_lock.lock().unwrap_or_else(|e| e.into_inner());

        let data = {
            let mut memtable = lock_write(&self.memtable);
            if memtable.is_empty() {
                debug!("flush called but memtable is empty, skipping");
                return Ok(());
            }

            let size = memtable.len();
            info!(memtable_entries = size, "flushing memtable to SSTable");

            let data = std::mem::take(&mut *memtable);
            self.frozen_memtable.store(Some(Arc::new(data.clone())));
            gauge!("lsm.memtable_entries").set(0.0);
            data
        };

        let start = Instant::now();

        let current_store_version = self.current_store_version.load();

        let path = Path::new(&self.data_directory)
            .join("level0")
            .join(current_store_version.level_0_ss_tables.len().to_string());
        let new_table = SsTable::new(data, &path, self.ss_table_block_size)?;

        let mut new_level_0_ss_tables = current_store_version.level_0_ss_tables.clone();
        new_level_0_ss_tables.push(new_table.into());

        let needs_compaction = new_level_0_ss_tables.len() == self.level_0_size;

        self.current_store_version.store(Arc::new(Storage {
            level_0_ss_tables: new_level_0_ss_tables,
            level_1_ss_tables: current_store_version.level_1_ss_tables.clone(),
        }));

        self.frozen_memtable.store(None);

        counter!("lsm.flushes").increment(1);
        histogram!("lsm.flush_duration_seconds").record(start.elapsed().as_secs_f64());
        gauge!("lsm.l0_tables")
            .set(self.current_store_version.load().level_0_ss_tables.len() as f64);

        info!("memtable flushed to L0 SSTable");

        if needs_compaction {
            debug!(
                l0_tables = self.level_0_size,
                "L0 full, triggering compaction"
            );
            self.compact_inner()?;
        }

        self.save_state()?;
        Ok(())
    }

    pub fn compact(&self) -> Result<(), Box<dyn Error>> {
        let _guard = self.store_lock.lock().unwrap_or_else(|e| e.into_inner());

        self.compact_inner()
    }

    /// Must be called with store_lock held.
    fn compact_inner(&self) -> Result<(), Box<dyn Error>> {
        let current = self.current_store_version.load();

        let l0_count = current.level_0_ss_tables.len();
        info!(l0_tables = l0_count, "starting L0 -> L1 compaction");

        let start = Instant::now();

        let iters = current
            .level_0_ss_tables
            .iter()
            .map(|ss_table| ss_table.iter())
            .collect::<Result<Vec<_>, _>>()?;

        // Actual compactions happen here, an implementation of FromIterator for BTreeMap takes
        // only the last (so most recent) item from the iterator.
        let compacted_level = iters
            .into_iter()
            .flatten()
            .collect::<Result<BTreeMap<K, Value<V>>, _>>()?;

        // TODO: We write all retained Tombstones here, without understanding if there were some
        //       values on level 1 before. Need to find a way to cleanup Tombstones without a
        //       corresponding values on deeper levels.

        let path = Path::new(&self.data_directory)
            .join("level1")
            .join(current.level_1_ss_tables.len().to_string());

        let new_lower_level_table = SsTable::new(compacted_level, &path, self.ss_table_block_size)?;

        let level_0 = Path::new(&self.data_directory).join("level0");

        for file in std::fs::read_dir(level_0)? {
            let file = file?;
            let path = file.path();
            trace!(path = %path.display(), "removing old L0 file");
            std::fs::remove_file(path)?;
        }

        let mut new_level_1 = current.level_1_ss_tables.clone();
        new_level_1.push(new_lower_level_table.into());

        self.current_store_version.store(Arc::new(Storage {
            level_0_ss_tables: Vec::new(),
            level_1_ss_tables: new_level_1,
        }));

        self.save_state()?;

        counter!("lsm.compactions").increment(1);
        histogram!("lsm.compaction_duration_seconds").record(start.elapsed().as_secs_f64());

        let store = self.current_store_version.load();
        gauge!("lsm.l0_tables").set(store.level_0_ss_tables.len() as f64);
        gauge!("lsm.l1_tables").set(store.level_1_ss_tables.len() as f64);

        info!(
            l0_compacted = l0_count,
            l1_tables = store.level_1_ss_tables.len(),
            "compaction complete"
        );

        Ok(())
    }

    fn save_state(&self) -> Result<(), Box<dyn Error>> {
        let mut writer =
            BufWriter::new(File::create(Path::new(&self.data_directory).join("state"))?);

        let current_store_version = self.current_store_version.load();

        let state = State {
            ss_table_block_size: self.ss_table_block_size,
            memtable_size: self.memtable_size,
            level_0_ss_tables: current_store_version.level_0_ss_tables.len(),
            level_1_ss_tables: current_store_version.level_1_ss_tables.len(),
            level_0_size: self.level_0_size,
        };

        debug!(
            l0_tables = state.level_0_ss_tables,
            l1_tables = state.level_1_ss_tables,
            "saving LSM tree state"
        );

        let encoded_state = bincode::encode_to_vec(state, bincode::config::standard())?;

        writer.write_all(encoded_state.as_slice())?;
        writer.flush()?;
        Ok(())
    }
}

fn lock_read<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|e| e.into_inner())
}

fn lock_write<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(|e| e.into_inner())
}
