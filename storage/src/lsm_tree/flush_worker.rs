use super::{
    compaction_worker::LsmTreeCompactionWorker, frozen_memtables::FrozenMemtables,
    lock_utils::lock_mutex, state::State, storage::Storage,
};
use crate::ss_table::SsTable;
use arc_swap::ArcSwap;
use dbcore::error::NornsDbError;
use metrics::{gauge, histogram};
use std::{
    fs::File,
    hash::Hash,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::{
        Arc,
        mpsc::{Receiver, SyncSender, TrySendError},
    },
    time::{Duration, Instant},
};
use tracing::{debug, error, info, warn};

pub(super) enum FlushWorkerCommand {
    Flush,
    FlushSync { tx: SyncSender<()> },
    Stop { tx: SyncSender<()> },
}

pub(super) struct FlushWorkerParams<K, V>
where
    K: Hash + Clone + Ord + bincode::Encode + bincode::Decode<()> + Send + Sync + 'static,
    V: Clone + bincode::Encode + bincode::Decode<()> + Send + Sync + 'static,
{
    pub(super) frozen_memtables: FrozenMemtables<K, V>,
    pub(super) data_directory: PathBuf,
    pub(super) ss_table_block_size: usize,
    pub(super) level_0_size: usize,
    pub(super) current_store_version: Arc<ArcSwap<Storage<K, V>>>,
    pub(super) metrics_labels: [(&'static str, String); 1],
    pub(super) memtable_size: usize,
    pub(super) compaction_worker: LsmTreeCompactionWorker<K, V>,
}

pub(super) struct LsmTreeFlushWorker<K, V>
where
    K: Hash + Clone + Ord + bincode::Encode + bincode::Decode<()> + Send + Sync + 'static,
    V: Clone + bincode::Encode + bincode::Decode<()> + Send + Sync + 'static,
{
    frozen_memtables: FrozenMemtables<K, V>,
    data_directory: PathBuf,
    ss_table_block_size: usize,
    level_0_size: usize,
    current_store_version: Arc<ArcSwap<Storage<K, V>>>,
    metrics_labels: [(&'static str, String); 1],
    memtable_size: usize,
    compaction_worker: LsmTreeCompactionWorker<K, V>,
    tx: SyncSender<FlushWorkerCommand>,
}

impl<K, V> Clone for LsmTreeFlushWorker<K, V>
where
    K: Hash + Clone + Ord + bincode::Encode + bincode::Decode<()> + Send + Sync + 'static,
    V: Clone + bincode::Encode + bincode::Decode<()> + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            frozen_memtables: self.frozen_memtables.clone(),
            data_directory: self.data_directory.clone(),
            ss_table_block_size: self.ss_table_block_size,
            level_0_size: self.level_0_size,
            current_store_version: self.current_store_version.clone(),
            metrics_labels: self.metrics_labels.clone(),
            memtable_size: self.memtable_size,
            compaction_worker: self.compaction_worker.clone(),
            tx: self.tx.clone(),
        }
    }
}

impl<K, V> LsmTreeFlushWorker<K, V>
where
    K: Hash + Clone + Ord + bincode::Encode + bincode::Decode<()> + Send + Sync + 'static,
    V: Clone + bincode::Encode + bincode::Decode<()> + Send + Sync + 'static,
{
    pub(super) fn spawn(params: FlushWorkerParams<K, V>) -> Self {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);

        let worker = Self {
            frozen_memtables: params.frozen_memtables,
            data_directory: params.data_directory,
            ss_table_block_size: params.ss_table_block_size,
            level_0_size: params.level_0_size,
            current_store_version: params.current_store_version,
            metrics_labels: params.metrics_labels,
            memtable_size: params.memtable_size,
            compaction_worker: params.compaction_worker,
            tx,
        };

        worker.run_thread(rx);

        worker
    }

    pub(super) fn flush(&self) -> Result<(), NornsDbError> {
        match self.tx.try_send(FlushWorkerCommand::Flush) {
            Ok(_) => Ok(()),
            Err(TrySendError::Full(_)) => {
                warn!("flush worker channel is full, flush request dropped");
                Ok(())
            }
            Err(e @ TrySendError::Disconnected(_)) => Err(NornsDbError::internal(
                e,
                "flush worker channel disconnected",
            )),
        }
    }

    pub(super) fn flush_sync(&self) -> Result<(), NornsDbError> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);

        if let Err(error) = self.tx.send(FlushWorkerCommand::FlushSync { tx }) {
            error!(
                ?error,
                "can't finish compaction: flush worker channel disconnected"
            );
            return Err(NornsDbError::internal(
                error,
                "can't finish compaction: flush worker channel disconnected",
            ));
        }

        let result = rx.recv_timeout(Duration::from_secs(10));
        if let Err(error) = result {
            error!(?error, "flush worker response timeout");
            return Err(NornsDbError::internal(
                error,
                "flush worker response timeout",
            ));
        }

        Ok(())
    }

    pub(super) fn stop(&self) -> Result<(), NornsDbError> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);

        if let Err(error) = self.tx.send(FlushWorkerCommand::Stop { tx }) {
            error!(
                ?error,
                "can't stop flush worker: flush worker channel disconnected"
            );
            return Err(NornsDbError::internal(
                error,
                "can't stop flush worker: channel disconnected",
            ));
        }

        let result = rx.recv_timeout(Duration::from_secs(10));
        if let Err(error) = result {
            error!(?error, "flush worker response timeout");
            return Err(NornsDbError::internal(
                error,
                "flush worker response timeout",
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

    fn run_thread(&self, rx: Receiver<FlushWorkerCommand>) {
        let worker = self.clone();

        std::thread::spawn(move || {
            use FlushWorkerCommand::*;

            loop {
                match rx.recv() {
                    Ok(Flush) => {
                        if let Err(error) = worker.flush_inner() {
                            error!(?error, "flush worker failed");
                        }
                    }
                    Ok(FlushSync { tx }) => {
                        if let Err(error) = worker.flush_inner() {
                            error!(?error, "flush worker failed");
                        }

                        if let Err(error) = tx.send(()) {
                            error!(?error, "flush worker failed: can't confirm sync flush");
                            return;
                        }
                    }
                    Ok(Stop { tx }) => {
                        if let Err(error) = tx.send(()) {
                            error!(
                                ?error,
                                "flush worker failed: can't confirm stop, terminating"
                            );
                            return;
                        }

                        info!("flush worker thread terminated");
                        return;
                    }
                    Err(error) => {
                        error!(?error, "flush worker thread terminated");
                        return;
                    }
                }
            }
        });
    }

    fn flush_inner(&self) -> Result<(), NornsDbError> {
        let (queue_mutex, condvar) = self.frozen_memtables.as_ref();

        let mut flushed_any = false;

        loop {
            let data = {
                let mut queue = lock_mutex(queue_mutex);
                queue.pop_front()
            };

            let Some(data) = data else { break };

            flushed_any = true;
            let start = Instant::now();
            info!("flushing frozen memtable to SSTable");

            let store = self.current_store_version.load();

            let path = self
                .data_directory
                .join("level0")
                .join(store.level_0_ss_tables.len().to_string());

            let new_table = Arc::new(SsTable::new(
                (*data).clone(),
                path,
                self.ss_table_block_size,
            )?);

            let mut new_level_0_ss_tables = store.level_0_ss_tables.clone();
            new_level_0_ss_tables.push(new_table);
            let new_level_0_count = new_level_0_ss_tables.len();

            self.current_store_version.store(Arc::new(Storage {
                level_0_ss_tables: new_level_0_ss_tables,
                level_1_ss_tables: store.level_1_ss_tables.clone(),
            }));

            condvar.notify_one();

            histogram!("norns_lsm_flush_duration_ms", &self.metrics_labels)
                .record(start.elapsed().as_millis() as f64);

            gauge!("norns_lsm_l0_tables", &self.metrics_labels).set(new_level_0_count as f64);

            info!("frozen memtable flushed to L0 SSTable");
        }

        if !flushed_any {
            debug!("flush called but frozen queue is empty, skipping");
            return Ok(());
        }

        let current_l0_count = self.current_store_version.load().level_0_ss_tables.len();
        if current_l0_count >= self.level_0_size {
            debug!(
                l0_tables = self.level_0_size,
                "L0 full, triggering compaction"
            );
            self.compaction_worker.compact()?;
        }

        self.save_state()?;

        Ok(())
    }
}
