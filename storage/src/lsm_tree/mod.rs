#[cfg(test)]
mod tests;

use crate::ss_table::SsTable;
use arc_swap::{ArcSwap, ArcSwapOption};
use dbcore::error::NornsDbError;
use itertools::Itertools;
use metrics::{gauge, histogram};
use std::{
    collections::{BTreeMap, HashSet},
    fs::File,
    hash::Hash,
    io::{BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard},
    time::Instant,
};
use tracing::{debug, info, instrument, trace};

pub struct LsmTree<K, V>
where
    K: Hash + Clone + Ord + bincode::Encode + bincode::Decode<()>,
    V: Clone + bincode::Encode + bincode::Decode<()>,
{
    table_name: String,
    memtable: RwLock<BTreeMap<K, Value<V>>>,
    memtable_size: usize,
    data_directory: PathBuf,
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
        debug!(dir = %self.data_directory.display(), "LSM tree shutting down, flushing memtable");
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
        table_name: String,
        data_directory: impl Into<PathBuf>,
        memtable_size: usize,
        level_0_size: usize,
        ss_table_block_size: usize,
    ) -> Result<Self, NornsDbError> {
        let data_directory = data_directory.into();

        assert!(
            !std::fs::exists(data_directory.join("state"))?,
            "LSM tree already initialized at {}, use load() instead",
            data_directory.display()
        );

        std::fs::create_dir_all(data_directory.join("level0"))?;
        std::fs::create_dir_all(data_directory.join("level1"))?;

        debug!(
            dir = %data_directory.display(),
            memtable_size,
            level_0_size,
            ss_table_block_size,
            "LSM tree created"
        );

        gauge!("norns_lsm_l0_tables", "table_name" => table_name.clone()).set(0.0);
        gauge!("norns_lsm_l1_tables", "table_name" => table_name.clone()).set(0.0);

        let tree = Self {
            table_name,
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
        };

        tree.save_state()?;
        Ok(tree)
    }

    pub fn load(
        table_name: String,
        data_directory: impl Into<PathBuf>,
    ) -> Result<Self, NornsDbError> {
        let data_directory = data_directory.into();
        debug!(dir = %data_directory.display(), "loading LSM tree");

        let reader = BufReader::new(File::open(data_directory.join("state"))?);

        let State {
            ss_table_block_size,
            memtable_size,
            level_0_ss_tables,
            level_1_ss_tables,
            level_0_size,
        }: State = bincode::decode_from_reader(reader, bincode::config::standard())?;

        debug!(
            dir = %data_directory.display(),
            memtable_size,
            level_0_size,
            ss_table_block_size,
            l0_tables = level_0_ss_tables,
            l1_tables = level_1_ss_tables,
            "LSM tree state loaded"
        );

        let level_0_ss_tables = Self::load_level(&data_directory, "level0", level_0_ss_tables)?;
        let level_1_ss_tables = Self::load_level(&data_directory, "level1", level_1_ss_tables)?;

        gauge!("norns_lsm_l0_tables", "table_name" => table_name.clone())
            .set(level_0_ss_tables.len() as f64);
        gauge!("norns_lsm_l1_tables", "table_name" => table_name.clone())
            .set(level_1_ss_tables.len() as f64);

        Ok(Self {
            table_name,
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

    #[instrument(skip(self))]
    pub fn list(&self) -> Result<Vec<(K, V)>, NornsDbError> {
        // FIXME: rework this mess with kinda Parallel Sequential Scan.

        let build_fs_iterators = |level: &Vec<Arc<SsTable<K, Value<V>>>>, level_priority: u8| {
            level.iter()
                .rev()
                .enumerate()
                .map(move |(table_priority, ss_table)| {
                    let iter = ss_table.iter()?.filter_map(|res| match res {
                        Ok((k, Value::Data(v))) => Some(Ok((k, v))),
                        Ok((_, Value::Tombstone)) => None,
                        Err(e) => Some(Err(e)),
                    });

                    Ok::<
                        Box<dyn Iterator<Item = (u8, usize, Result<(K, V), NornsDbError>)>>,
                        NornsDbError,
                    >(
                        Box::new(iter.map(move |kv| (level_priority, table_priority, kv))) as _
                    )
                })
                .collect::<Result<
                    Vec<Box<dyn Iterator<Item = (u8, usize, Result<(K, V), NornsDbError>)>>>,
                    _,
                >>()
        };

        let build_inmemory_iterator = |memtable: BTreeMap<K, Value<V>>, priority: usize| {
            Box::new(
                memtable
                    .clone()
                    .into_iter()
                    .filter_map(|kv| match kv {
                        (k, Value::Data(v)) => Some(Ok::<_, NornsDbError>((k, v))),
                        (_, Value::Tombstone) => None,
                    })
                    .map(move |kv| (0u8, priority, kv)),
            ) as Box<dyn Iterator<Item = (u8, usize, Result<(K, V), NornsDbError>)>>
        };

        let memtable = {
            let memtable = lock_read(&self.memtable);
            memtable.clone()
        };

        let memtable_iterator = build_inmemory_iterator(memtable, 0);

        let frozen_memtable = {
            let frozen_memtable = self.frozen_memtable.load();
            frozen_memtable.as_ref().map(|mt| (**mt).clone())
        };

        let frozen_memtable_iterator = frozen_memtable
            .map(|mt| build_inmemory_iterator(mt, 1))
            .unwrap_or_else(|| {
                Box::new(std::iter::empty())
                    as Box<dyn Iterator<Item = (u8, usize, Result<(K, V), NornsDbError>)>>
            });

        let current_store_version = self.current_store_version.load();

        let level0_iterators = build_fs_iterators(&current_store_version.level_0_ss_tables, 0)?;
        let level1_iterators = build_fs_iterators(&current_store_version.level_1_ss_tables, 1)?;

        let mut iterators = vec![memtable_iterator, frozen_memtable_iterator];
        iterators.extend(level0_iterators);
        iterators.extend(level1_iterators);

        iterators
            .into_iter()
            .kmerge_by(|(level0, p0, kv0), (level1, p1, kv1)| {
                let k0 = kv0.as_ref().ok().map(|(k, _)| k);
                let k1 = kv1.as_ref().ok().map(|(k, _)| k);

                if k0 < k1 {
                    return true;
                }

                if level0 < level1 {
                    return true;
                }

                p0 < p1
            })
            .map(|(_, _, kv)| kv)
            .try_fold(
                (Vec::new(), HashSet::new()),
                |(mut items, mut keys_met), item_result| {
                    let (key, val) = item_result?;

                    if keys_met.insert(key.clone()) {
                        items.push((key, val))
                    }

                    Ok((items, keys_met))
                },
            )
            .map(|(items, _)| items)
    }

    #[instrument(skip(self, key, value))]
    pub fn insert(&self, key: K, value: V) -> Result<(), NornsDbError> {
        let start = Instant::now();

        let needs_flush = {
            let memtable = lock_read(&self.memtable);
            memtable.len() >= self.memtable_size
        };

        if needs_flush {
            debug!("memtable full, triggering flush before insert");
            self.flush()?;
        }

        trace!("inserting key into memtable");
        let _size = {
            let mut memtable = lock_write(&self.memtable);
            memtable.insert(key, Value::Data(value));
            memtable.len()
        };

        histogram!("norns_lsm_insert_duration_us", "table_name" => self.table_name.clone())
            .record(start.elapsed().as_micros() as f64);

        Ok(())
    }

    #[instrument(skip(self, key))]
    pub fn get(&self, key: &K) -> Result<Option<V>, NornsDbError> {
        let start = Instant::now();

        let result = (|| {
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

            if let Some(value) =
                self.lookup_in_level(&current_store_version.level_0_ss_tables, key)?
            {
                trace!("key found in L0 SSTables");
                return Ok(Some(value));
            }

            if let Some(value) =
                self.lookup_in_level(&current_store_version.level_1_ss_tables, key)?
            {
                trace!("key found in L1 SSTables");
                return Ok(Some(value));
            }

            trace!("key not found in any level");
            Ok(None)
        })();

        histogram!("norns_lsm_get_duration_us", "table_name" => self.table_name.clone())
            .record(start.elapsed().as_micros() as f64);

        result
    }

    fn lookup_in_level(
        &self,
        level: &[Arc<SsTable<K, Value<V>>>],
        key: &K,
    ) -> Result<Option<V>, NornsDbError> {
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

    pub fn delete(&self, key: K) -> Result<Option<V>, NornsDbError> {
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
        let mut memtable = lock_write(&self.memtable);
        memtable.insert(key, Value::Tombstone);

        Ok(value)
    }

    pub fn flush(&self) -> Result<(), NornsDbError> {
        let start = Instant::now();

        let _guard = self.store_lock.lock().unwrap_or_else(|e| e.into_inner());

        let data = {
            let mut memtable = lock_write(&self.memtable);
            if memtable.is_empty() {
                debug!("flush called but memtable is empty, skipping");
                return Ok(());
            }

            info!("flushing memtable to SSTable");

            let data = std::mem::take(&mut *memtable);
            self.frozen_memtable.store(Some(Arc::new(data.clone())));
            data
        };

        let current_store_version = self.current_store_version.load();

        let path = self
            .data_directory
            .join("level0")
            .join(current_store_version.level_0_ss_tables.len().to_string());

        let new_table = SsTable::new(data, &path, self.ss_table_block_size)?;

        let mut new_level_0_ss_tables = current_store_version.level_0_ss_tables.clone();
        new_level_0_ss_tables.push(new_table.into());
        let level0_size = new_level_0_ss_tables.len();

        let needs_compaction = new_level_0_ss_tables.len() == self.level_0_size;

        self.current_store_version.store(Arc::new(Storage {
            level_0_ss_tables: new_level_0_ss_tables,
            level_1_ss_tables: current_store_version.level_1_ss_tables.clone(),
        }));

        self.frozen_memtable.store(None);

        histogram!("norns_lsm_flush_duration_ms", "table_name" => self.table_name.clone())
            .record(start.elapsed().as_millis() as f64);

        gauge!("norns_lsm_l0_tables", "table_name" => self.table_name.clone())
            .set(level0_size as f64);

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

    pub fn compact(&self) -> Result<(), NornsDbError> {
        let _guard = self.store_lock.lock().unwrap_or_else(|e| e.into_inner());

        self.compact_inner()
    }

    // FIXME: deal with this later
    #[allow(clippy::type_complexity)]
    fn load_level(
        data_directory: &Path,
        level_name: &str,
        tables_number: usize,
    ) -> Result<Vec<Arc<SsTable<K, Value<V>>>>, NornsDbError> {
        let mut level_ss_tables = Vec::with_capacity(tables_number);

        for ss_table in 0..tables_number {
            let path = data_directory.join(level_name).join(ss_table.to_string());

            let table = Arc::new(SsTable::<K, Value<V>>::load(path)?);
            level_ss_tables.push(table);
        }
        Ok(level_ss_tables)
    }

    /// Must be called with store_lock held.
    fn compact_inner(&self) -> Result<(), NornsDbError> {
        let start = Instant::now();

        let current = self.current_store_version.load();

        let l0_count = current.level_0_ss_tables.len();
        info!(l0_tables = l0_count, "starting L0 -> L1 compaction");

        let iters = current
            .level_0_ss_tables
            .iter()
            .map(|ss_table| ss_table.iter())
            .collect::<Result<Vec<_>, _>>()?;

        // Actual compaction happens here, an implementation of FromIterator for BTreeMap takes
        // only the last (so most recent) item from the iterator.
        let compacted_level = iters
            .into_iter()
            .flatten()
            .collect::<Result<BTreeMap<K, Value<V>>, _>>()?;

        // TODO: We write all retained Tombstones here, without understanding if there were some
        //       values on level 1 before. Need to find a way to cleanup Tombstones without a
        //       corresponding values on deeper levels.

        let path = self
            .data_directory
            .join("level1")
            .join(current.level_1_ss_tables.len().to_string());

        let new_lower_level_table = SsTable::new(compacted_level, &path, self.ss_table_block_size)?;

        let level_0 = self.data_directory.join("level0");

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

        histogram!("norns_lsm_l0_compaction_duration_ms", "table_name" => self.table_name.clone())
            .record(start.elapsed().as_millis() as f64);

        let store = self.current_store_version.load();
        gauge!("norns_lsm_l0_tables", "table_name" => self.table_name.clone())
            .set(store.level_0_ss_tables.len() as f64);

        gauge!("norns_lsm_l1_tables", "table_name" => self.table_name.clone())
            .set(store.level_1_ss_tables.len() as f64);

        info!(
            l0_compacted = l0_count,
            l1_tables = store.level_1_ss_tables.len(),
            "compaction complete"
        );

        Ok(())
    }

    pub fn destroy(self) -> Result<(), NornsDbError> {
        // Prevent from flushing memtable to a disk.
        {
            lock_write(&self.memtable).clear();
        }

        let dir = self.data_directory.clone();
        drop(self);
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    fn save_state(&self) -> Result<(), NornsDbError> {
        let tmp_path = self.data_directory.join("state.tmp");

        let temp_file = File::create(&tmp_path)?;
        let mut writer = BufWriter::new(&temp_file);

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
        temp_file.sync_all()?;

        std::fs::rename(tmp_path, self.data_directory.join("state"))?;
        Ok(())
    }
}

fn lock_read<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|e| e.into_inner())
}

fn lock_write<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(|e| e.into_inner())
}
