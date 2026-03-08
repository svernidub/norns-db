use crate::journal_record::JournalRecord;
use dbcore::error::NornsDbError;
use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    marker::PhantomData,
    path::{Path, PathBuf},
};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, trace};

pub struct Journal<K, V> {
    sender: mpsc::Sender<WriteRequest>,
    #[allow(dead_code)]
    path: PathBuf,
    _marker: PhantomData<(K, V)>,
}

struct WriteRequest {
    payload: Vec<u8>,
    done: oneshot::Sender<Result<(), NornsDbError>>,
}

impl<K, V> Journal<K, V> {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, NornsDbError> {
        let path = path.as_ref().to_path_buf();

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| {
                error!(path = %path.display(), error = %e, "failed to open journal file");
                NornsDbError::unknown_with_message(e, "Failed to open journal file")
            })?;

        // TODO: make configurable?
        let (sender, receiver) = mpsc::channel(256);

        let writer_path = path.clone();
        tokio::task::spawn_blocking(move || {
            Self::run_writer_loop(file, receiver, &writer_path);
        });

        debug!(path = %path.display(), "journal opened");

        Ok(Self {
            sender,
            path,
            _marker: PhantomData,
        })
    }

    pub async fn append(&self, record: &JournalRecord<K, V>) -> Result<(), NornsDbError>
    where
        K: bincode::Encode,
        V: bincode::Encode,
    {
        let payload = bincode::encode_to_vec(record, bincode::config::standard()).map_err(|e| {
            error!(error = %e, "failed to encode journal record");
            NornsDbError::unknown_with_message(e, "Failed to encode journal record")
        })?;

        let (done_tx, done_rx) = oneshot::channel::<Result<(), NornsDbError>>();

        debug!(payload_size = payload.len(), "appending journal record");

        self.sender
            .send(WriteRequest {
                payload,
                done: done_tx,
            })
            .await
            .map_err(|e| {
                error!(error = %e, "failed to send journal record to writer");
                NornsDbError::unknown_with_message(e, "Failed to send journal record")
            })?;

        done_rx.await.map_err(|e| {
            error!(error = %e, "journal writer dropped without responding");
            NornsDbError::unknown_with_message(e, "Failed to process journal record")
        })?
    }

    fn run_writer_loop(file: File, mut receiver: mpsc::Receiver<WriteRequest>, path: &Path) {
        debug!(path = %path.display(), "journal writer loop started");
        let mut writer = BufWriter::new(file);

        while let Some(request) = receiver.blocking_recv() {
            let result = Self::write_record(&mut writer, &request.payload);
            let _ = request.done.send(result);
        }

        debug!(path = %path.display(), "journal writer loop stopped");
    }

    // Write a record to the journal with the next layout:
    // [4 bytes length][length bytes payload][4 bytes checksum].
    // Immediately flushes the buffer.
    fn write_record(writer: &mut BufWriter<File>, payload: &[u8]) -> Result<(), NornsDbError> {
        let len = payload.len() as u32;
        let checksum = crc_fast::crc32_iscsi(payload);

        trace!(payload_len = len, checksum, "writing journal record");

        writer
            .write_all(&len.to_le_bytes())
            .and_then(|_| writer.write_all(payload))
            .and_then(|_| writer.write_all(&checksum.to_le_bytes()))
            .and_then(|_| writer.flush())
            .and_then(|_| writer.get_mut().sync_all())
            .map_err(|e| {
                error!(error = %e, "failed to write journal record to disk");
                NornsDbError::unknown_with_message(e, "Failed to write journal record")
            })
    }
}
