mod compaction_worker;
mod flush_worker;
mod frozen_memtables;
mod lock_utils;
mod state;
mod storage;
#[cfg(test)]
mod tests;
mod value;

use crate::ss_table::SsTable;
use arc_swap::ArcSwap;
use compaction_worker::{CompactionWorkerParams, LsmTreeCompactionWorker};
use dbcore::error::NornsDbError;
use flush_worker::{FlushWorkerParams, LsmTreeFlushWorker};
use frozen_memtables::FrozenMemtables;
use itertools::Itertools;
use lock_utils::{lock_mutex, lock_read, lock_write};
use metrics::{gauge, histogram};
use state::State;
use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    fs::File,
    hash::Hash,
    io::BufReader,
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex, RwLock},
    time::Instant,
};
use storage::Storage;
use tracing::{debug, instrument, trace};
use value::Value;

pub struct LsmTree<K, V>
where
    K: Hash + Clone + Ord + bincode::Encode + bincode::Decode<()> + Send + Sync + 'static,
    V: Clone + bincode::Encode + bincode::Decode<()> + Send + Sync + 'static,
{
    memtable: Arc<RwLock<BTreeMap<K, Value<V>>>>,
    memtable_size: usize,
    data_directory: PathBuf,
    max_frozen_memtables: usize,

    // Queue of frozen (full) memtables waiting to be flushed to L0 SSTables.
    // The Condvar is notified after each flush, so writers waiting a slot in a full queue can
    // proceed.
    frozen_memtables: FrozenMemtables<K, V>,

    compaction_worker: LsmTreeCompactionWorker<K, V>,
    flush_worker: LsmTreeFlushWorker<K, V>,
    current_store_version: Arc<ArcSwap<Storage<K, V>>>,

    metrics_labels: [(&'static str, String); 1],
}

impl<K, V> Drop for LsmTree<K, V>
where
    K: Hash + Clone + Ord + bincode::Encode + bincode::Decode<()> + Send + Sync + 'static,
    V: Clone + bincode::Encode + bincode::Decode<()> + Send + Sync + 'static,
{
    fn drop(&mut self) {
        debug!(dir = %self.data_directory.display(), "LSM tree shutting down, flushing memtable");

        if let Err(e) = self.flush_sync() {
            debug!(error = %e, "failed to flush memtable on drop");
        }

        if let Err(e) = self.stop() {
            debug!(error = %e, "failed to stop flush worker on drop");
        }
    }
}

impl<K, V> LsmTree<K, V>
where
    K: Hash + Clone + Ord + bincode::Encode + bincode::Decode<()> + Send + Sync + 'static,
    V: Clone + bincode::Encode + bincode::Decode<()> + Send + Sync + 'static,
{
    pub fn new(
        table_name: String,
        data_directory: impl Into<PathBuf>,
        memtable_size: usize,
        level_0_size: usize,
        ss_table_block_size: usize,
        max_frozen_memtables: usize,
    ) -> Result<Self, NornsDbError> {
        let data_directory = data_directory.into();

        debug_assert!(
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
            max_frozen_memtables,
            "LSM tree created"
        );

        let metrics_labels = [("table_name", table_name)];

        gauge!("norns_lsm_l0_tables", &metrics_labels).set(0.0);
        gauge!("norns_lsm_l1_tables", &metrics_labels).set(0.0);

        let frozen_memtables = Arc::new((Mutex::new(VecDeque::new()), Condvar::new()));
        let current_store_version = Arc::new(ArcSwap::from_pointee(Storage {
            level_0_ss_tables: vec![],
            level_1_ss_tables: vec![],
        }));
        let version_lock = Arc::new(Mutex::new(()));

        let compaction_worker = LsmTreeCompactionWorker::spawn(CompactionWorkerParams {
            data_directory: data_directory.clone(),
            ss_table_block_size,
            level_0_size,
            current_store_version: current_store_version.clone(),
            metrics_labels: metrics_labels.clone(),
            memtable_size,
            version_lock: version_lock.clone(),
        });

        let flush_worker = LsmTreeFlushWorker::spawn(FlushWorkerParams {
            frozen_memtables: frozen_memtables.clone(),
            data_directory: data_directory.clone(),
            ss_table_block_size,
            level_0_size,
            current_store_version: current_store_version.clone(),
            metrics_labels: metrics_labels.clone(),
            memtable_size,
            compaction_worker: compaction_worker.clone(),
            version_lock,
        });

        let tree = Self {
            metrics_labels,
            memtable: Arc::new(RwLock::new(BTreeMap::new())),
            memtable_size,
            data_directory,
            current_store_version,
            max_frozen_memtables,
            frozen_memtables,
            compaction_worker,
            flush_worker,
        };

        tree.flush_worker.save_state()?;
        Ok(tree)
    }

    pub fn load(
        table_name: String,
        data_directory: impl Into<PathBuf>,
        max_frozen_memtables: usize,
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
            max_frozen_memtables,
            l0_tables = level_0_ss_tables,
            l1_tables = level_1_ss_tables,
            "LSM tree state loaded"
        );

        let level_0_ss_tables = Self::load_level(&data_directory, "level0", level_0_ss_tables)?;
        let level_1_ss_tables = Self::load_level(&data_directory, "level1", level_1_ss_tables)?;

        let metrics_labels = [("table_name", table_name)];

        gauge!("norns_lsm_l0_tables", &metrics_labels).set(level_0_ss_tables.len() as f64);
        gauge!("norns_lsm_l1_tables", &metrics_labels).set(level_1_ss_tables.len() as f64);

        let frozen_memtables = Arc::new((Mutex::new(VecDeque::new()), Condvar::new()));
        let current_store_version = Arc::new(ArcSwap::from_pointee(Storage {
            level_0_ss_tables,
            level_1_ss_tables,
        }));
        let version_lock = Arc::new(Mutex::new(()));

        let compaction_worker = LsmTreeCompactionWorker::spawn(CompactionWorkerParams {
            data_directory: data_directory.clone(),
            ss_table_block_size,
            level_0_size,
            current_store_version: current_store_version.clone(),
            metrics_labels: metrics_labels.clone(),
            memtable_size,
            version_lock: version_lock.clone(),
        });

        let flush_worker = LsmTreeFlushWorker::spawn(FlushWorkerParams {
            frozen_memtables: frozen_memtables.clone(),
            data_directory: data_directory.clone(),
            ss_table_block_size,
            level_0_size,
            current_store_version: current_store_version.clone(),
            metrics_labels: metrics_labels.clone(),
            memtable_size,
            compaction_worker: compaction_worker.clone(),
            version_lock,
        });

        let tree = Self {
            metrics_labels,
            memtable: Arc::new(RwLock::new(BTreeMap::new())),
            memtable_size,
            data_directory,
            max_frozen_memtables,
            frozen_memtables,
            compaction_worker,
            flush_worker,
            current_store_version,
        };

        Ok(tree)
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

        let frozen_snapshot: Vec<Arc<BTreeMap<K, Value<V>>>> = {
            let (queue_mutex, _) = self.frozen_memtables.as_ref();
            lock_mutex(queue_mutex).iter().cloned().collect()
        };
        let frozen_iterators: Vec<_> = frozen_snapshot
            .iter()
            .rev()
            .enumerate()
            .map(|(i, frozen)| build_inmemory_iterator((**frozen).clone(), i + 1))
            .collect();

        let current_store_version = self.current_store_version.load();

        let level0_iterators = build_fs_iterators(&current_store_version.level_0_ss_tables, 0)?;
        let level1_iterators = build_fs_iterators(&current_store_version.level_1_ss_tables, 1)?;

        let mut iterators = vec![memtable_iterator];
        iterators.extend(frozen_iterators);
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
        trace!("inserting key into memtable");

        self.write_value(key, Value::Data(value))?;

        histogram!("norns_lsm_insert_duration_us", &self.metrics_labels)
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

            let frozen_snapshot: Vec<Arc<BTreeMap<K, Value<V>>>> = {
                let (queue_mutex, _) = self.frozen_memtables.as_ref();
                lock_mutex(queue_mutex).iter().cloned().collect()
            };
            for frozen in frozen_snapshot.iter().rev() {
                if let Some(value) = frozen.get(key) {
                    trace!("key found in frozen memtable");
                    return match value {
                        Value::Data(d) => Ok(Some(d.clone())),
                        Value::Tombstone => Ok(None),
                    };
                }
            }

            let current_store_version = self.current_store_version.load();

            if let Some(maybe_value) =
                self.lookup_in_level(&current_store_version.level_0_ss_tables, key)?
            {
                trace!("key found in L0 SSTables");
                return Ok(maybe_value);
            }

            if let Some(maybe_value) =
                self.lookup_in_level(&current_store_version.level_1_ss_tables, key)?
            {
                trace!("key found in L1 SSTables");
                return Ok(maybe_value);
            }

            trace!("key not found in any level");
            Ok(None)
        })();

        histogram!("norns_lsm_get_duration_us", &self.metrics_labels)
            .record(start.elapsed().as_micros() as f64);

        result
    }

    #[instrument(skip(self, key))]
    pub fn delete(&self, key: K) -> Result<(), NornsDbError> {
        let start = Instant::now();
        debug!("writing tombstone for key");

        self.write_value(key, Value::Tombstone)?;

        histogram!("norns_lsm_delete_duration_us", &self.metrics_labels)
            .record(start.elapsed().as_micros() as f64);

        Ok(())
    }

    pub fn flush(&self) -> Result<(), NornsDbError> {
        self.flush_worker.flush()
    }

    pub fn flush_sync(&self) -> Result<(), NornsDbError> {
        // Rotate the active memtable into the frozen queue if it has data.
        // This ensures flush_sync persists everything, not just previously rotated memtables.
        {
            let mut memtable = lock_write(&self.memtable);
            if !memtable.is_empty() {
                let (queue_mutex, condvar) = self.frozen_memtables.as_ref();
                let mut queue = lock_mutex(queue_mutex);
                while queue.len() >= self.max_frozen_memtables {
                    queue = condvar.wait(queue).unwrap_or_else(|e| e.into_inner());
                }
                let frozen = Arc::new(std::mem::take(&mut *memtable));
                queue.push_back(frozen);
            }
        }

        self.flush_worker.flush_sync()
    }

    pub fn compact_sync(&self) -> Result<(), NornsDbError> {
        self.compaction_worker.compact_sync()
    }

    pub fn destroy(self) -> Result<(), NornsDbError> {
        let dir = self.data_directory.clone();
        drop(self);
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    fn write_value(&self, key: K, value: Value<V>) -> Result<(), NornsDbError> {
        let should_flush = {
            let mut memtable = lock_write(&self.memtable);
            memtable.insert(key, value);
            if memtable.len() >= self.memtable_size {
                let (queue_mutex, condvar) = self.frozen_memtables.as_ref();
                let mut queue = lock_mutex(queue_mutex);
                while queue.len() >= self.max_frozen_memtables {
                    queue = condvar.wait(queue).unwrap_or_else(|e| e.into_inner());
                }
                let frozen = Arc::new(std::mem::take(&mut *memtable));
                queue.push_back(frozen);
                true
            } else {
                false
            }
        };

        if should_flush {
            debug!("memtable full, triggering flush");
            self.flush()?;
        }

        Ok(())
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

    fn lookup_in_level(
        &self,
        level: &[Arc<SsTable<K, Value<V>>>],
        key: &K,
    ) -> Result<Option<Option<V>>, NornsDbError> {
        for ss_table in level.iter().rev() {
            if let Some(value) = ss_table.get(key)? {
                return match value {
                    Value::Data(d) => Ok(Some(Some(d))),
                    Value::Tombstone => Ok(Some(None)),
                };
            }
        }

        Ok(None)
    }

    fn stop(&self) -> Result<(), NornsDbError> {
        self.flush_worker.stop()?;
        self.compaction_worker.stop()
    }
}
