use super::{lock_utils::lock_mutex, state::State, storage::Storage, value::Value};
use crate::ss_table::SsTable;
use arc_swap::ArcSwap;
use dbcore::error::NornsDbError;
use metrics::{gauge, histogram};
use std::{
    collections::BTreeMap,
    fs::File,
    hash::Hash,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        mpsc::{Receiver, SyncSender, TrySendError},
    },
    time::{Duration, Instant},
};
use tracing::{debug, error, info, trace, warn};

pub(super) struct LsmTreeCompactionWorker<K, V>
where
    K: Hash + Clone + Ord + bincode::Encode + bincode::Decode<()> + Send + Sync + 'static,
    V: Clone + bincode::Encode + bincode::Decode<()> + Send + Sync + 'static,
{
    current_store_version: Arc<ArcSwap<Storage<K, V>>>,
    data_directory: PathBuf,
    ss_table_block_size: usize,
    level_0_size: usize,
    memtable_size: usize,
    metrics_labels: [(&'static str, String); 1],
    version_lock: Arc<Mutex<()>>,
    tx: SyncSender<CompactionWorkerCommand>,
}

pub(super) struct CompactionWorkerParams<K, V>
where
    K: Hash + Clone + Ord + bincode::Encode + bincode::Decode<()> + Send + Sync + 'static,
    V: Clone + bincode::Encode + bincode::Decode<()> + Send + Sync + 'static,
{
    pub data_directory: PathBuf,
    pub ss_table_block_size: usize,
    pub level_0_size: usize,
    pub current_store_version: Arc<ArcSwap<Storage<K, V>>>,
    pub metrics_labels: [(&'static str, String); 1],
    pub memtable_size: usize,
    pub version_lock: Arc<Mutex<()>>,
}

pub(super) enum CompactionWorkerCommand {
    Compact,
    CompactSync { tx: SyncSender<()> },
    Stop { tx: SyncSender<()> },
}

impl<K, V> Clone for LsmTreeCompactionWorker<K, V>
where
    K: Hash + Clone + Ord + bincode::Encode + bincode::Decode<()> + Send + Sync + 'static,
    V: Clone + bincode::Encode + bincode::Decode<()> + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            current_store_version: self.current_store_version.clone(),
            data_directory: self.data_directory.clone(),
            ss_table_block_size: self.ss_table_block_size,
            level_0_size: self.level_0_size,
            memtable_size: self.memtable_size,
            metrics_labels: self.metrics_labels.clone(),
            version_lock: self.version_lock.clone(),
            tx: self.tx.clone(),
        }
    }
}

impl<K, V> LsmTreeCompactionWorker<K, V>
where
    K: Hash + Clone + Ord + bincode::Encode + bincode::Decode<()> + Send + Sync + 'static,
    V: Clone + bincode::Encode + bincode::Decode<()> + Send + Sync + 'static,
{
    pub(super) fn spawn(params: CompactionWorkerParams<K, V>) -> Self {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);

        let worker = Self {
            current_store_version: params.current_store_version,
            data_directory: params.data_directory,
            ss_table_block_size: params.ss_table_block_size,
            level_0_size: params.level_0_size,
            memtable_size: params.memtable_size,
            metrics_labels: params.metrics_labels,
            version_lock: params.version_lock,
            tx,
        };

        worker.run_thread(rx);

        worker
    }

    pub(super) fn compact(&self) -> Result<(), NornsDbError> {
        match self.tx.try_send(CompactionWorkerCommand::Compact) {
            Ok(_) => Ok(()),
            Err(TrySendError::Full(_)) => {
                warn!("compaction worker channel is full, compaction request dropped");
                Ok(())
            }
            Err(e @ TrySendError::Disconnected(_)) => Err(NornsDbError::internal(
                e,
                "compaction worker channel disconnected",
            )),
        }
    }

    pub(super) fn compact_sync(&self) -> Result<(), NornsDbError> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);

        if let Err(error) = self.tx.send(CompactionWorkerCommand::CompactSync { tx }) {
            error!(
                ?error,
                "can't sync compaction: compaction worker channel disconnected"
            );
            return Err(NornsDbError::internal(
                error,
                "can't sync compaction: compaction worker channel disconnected",
            ));
        }

        let result = rx.recv_timeout(Duration::from_secs(10));
        if let Err(error) = result {
            error!(?error, "compaction worker sync timeout");
            return Err(NornsDbError::internal(
                error,
                "compaction worker sync timeout",
            ));
        }

        Ok(())
    }

    pub(super) fn stop(&self) -> Result<(), NornsDbError> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);

        if let Err(error) = self.tx.send(CompactionWorkerCommand::Stop { tx }) {
            error!(
                ?error,
                "can't stop compaction worker: compaction worker channel disconnected"
            );
            return Err(NornsDbError::internal(
                error,
                "can't stop compaction worker: channel disconnected",
            ));
        }

        let result = rx.recv_timeout(Duration::from_secs(10));
        if let Err(error) = result {
            error!(?error, "compaction worker response timeout");
            return Err(NornsDbError::internal(
                error,
                "compaction worker response timeout",
            ));
        }

        Ok(())
    }

    pub(super) fn save_state(&self) -> Result<(), NornsDbError> {
        let tmp_path = self.data_directory.join("state.tmp");

        let temp_file = File::create(&tmp_path)?;
        let mut writer = BufWriter::new(&temp_file);

        let current = self.current_store_version.load();

        let state = State {
            ss_table_block_size: self.ss_table_block_size,
            memtable_size: self.memtable_size,
            level_0_ss_tables: current.level_0_ss_tables.len(),
            level_1_ss_tables: current.level_1_ss_tables.len(),
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

    fn run_thread(&self, rx: Receiver<CompactionWorkerCommand>) {
        let worker = self.clone();

        std::thread::spawn(move || {
            use CompactionWorkerCommand::*;

            loop {
                match rx.recv() {
                    Ok(Compact) => {
                        if let Err(error) = worker.perform_compaction() {
                            error!(?error, "compaction worker failed");
                        }
                    }
                    Ok(CompactSync { tx }) => {
                        if let Err(error) = worker.perform_compaction() {
                            error!(?error, "compaction worker failed");
                        }

                        if let Err(error) = tx.send(()) {
                            error!(
                                ?error,
                                "compaction worker failed: can't confirm sync, terminating"
                            );
                            return;
                        }
                    }
                    Ok(Stop { tx }) => {
                        if let Err(error) = tx.send(()) {
                            error!(
                                ?error,
                                "compaction worker failed: can't confirm stop, terminating"
                            );
                            return;
                        }

                        info!("compaction worker thread terminated");
                        return;
                    }
                    Err(error) => {
                        error!(?error, "compaction worker thread terminated");
                        return;
                    }
                }
            }
        });
    }

    fn perform_compaction(&self) -> Result<(), NornsDbError> {
        let _version_guard = lock_mutex(&self.version_lock);

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

        let new_lower_level_table = SsTable::new(compacted_level, path, self.ss_table_block_size)?;

        let level_0 = self.data_directory.join("level0");

        for file in std::fs::read_dir(level_0)? {
            let file = file?;
            let path = file.path();
            trace!(path = %path.display(), "removing old L0 file");
            std::fs::remove_file(path)?;
        }

        let mut new_level_1 = current.level_1_ss_tables.clone();
        new_level_1.push(new_lower_level_table.into());

        let level_1_ss_tables = new_level_1.len();

        self.current_store_version.store(Arc::new(Storage {
            level_0_ss_tables: Vec::new(),
            level_1_ss_tables: new_level_1,
        }));

        self.save_state()?;

        histogram!("norns_lsm_l0_compaction_duration_ms", &self.metrics_labels)
            .record(start.elapsed().as_millis() as f64);

        gauge!("norns_lsm_l0_tables", &self.metrics_labels).set(0);

        gauge!("norns_lsm_l1_tables", &self.metrics_labels).set(level_1_ss_tables as f64);

        info!(
            l0_compacted = l0_count,
            l1_tables = level_1_ss_tables,
            "compaction complete"
        );

        Ok(())
    }
}
