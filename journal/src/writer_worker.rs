use crate::JournalHandle;
use dbcore::error::NornsDbError;
use std::{
    fs::{File, OpenOptions},
    io::{Seek, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, oneshot};
use tracing::*;

pub(crate) struct WriterWorker {
    sender: mpsc::Sender<WriterRequest>,
}

enum WriterRequest {
    Entry {
        payload: Vec<u8>,
        done: oneshot::Sender<Result<JournalHandle, NornsDbError>>,
    },
    Commit {
        handle: JournalHandle,
        done: oneshot::Sender<Result<(), NornsDbError>>,
    },
    Shutdown {
        done: oneshot::Sender<()>,
    },
}

impl WriterWorker {
    pub fn new(file: File, path: PathBuf) -> Self {
        let (sender, receiver) = mpsc::channel(1);

        tokio::task::spawn_blocking(move || Self::run_writer_loop(file, receiver, path));

        Self { sender }
    }

    pub async fn insert_entry(&self, payload: Vec<u8>) -> Result<JournalHandle, NornsDbError> {
        let (tx, rx) = oneshot::channel();

        self.sender
            .send(WriterRequest::Entry { payload, done: tx })
            .await
            .map_err(|e| NornsDbError::internal(e, "cannot append an entry to journal"))?;

        rx.await
            .map_err(|e| NornsDbError::internal(e, "cannot confirm an entry addition to journal"))?
    }

    pub async fn commit(&self, handle: JournalHandle) -> Result<(), NornsDbError> {
        let (tx, rx) = oneshot::channel();

        self.sender
            .send(WriterRequest::Commit { handle, done: tx })
            .await
            .map_err(|e| NornsDbError::internal(e, "cannot commit a journal"))?;

        rx.await
            .map_err(|e| NornsDbError::internal(e, "cannot confirm a journal commit"))?
    }

    pub async fn shutdown(self) -> Result<(), NornsDbError> {
        let (tx, rx) = oneshot::channel();

        self.sender
            .send(WriterRequest::Shutdown { done: tx })
            .await
            .map_err(|e| NornsDbError::internal(e, "cannot shutdown a journal writer"))?;

        rx.await
            .map_err(|e| NornsDbError::internal(e, "cannot confirm a journal writer shutdown"))
    }

    fn run_writer_loop(mut file: File, mut receiver: mpsc::Receiver<WriterRequest>, path: PathBuf) {
        use WriterRequest::*;

        info!(path = %path.display(), "journal writer loop started");

        loop {
            match receiver.blocking_recv() {
                Some(Entry { payload, done }) => {
                    let result = Self::write_record(&mut file, payload);
                    let _ = done.send(result);
                }
                Some(Shutdown { done }) => {
                    info!(path = %path.display(), "journal writer loop stopped");
                    let _ = done.send(());
                    return;
                }
                Some(Commit { handle, done }) => {
                    let result = Self::commit_journal(&path, handle);
                    let _ = done.send(result);
                }
                None => {
                    error!(path = %path.display(), "journal writer loop channel closed, shutting down");
                    return;
                }
            }
        }
    }

    /// Writes a record to the journal with the next layout:
    ///
    /// ```text
    /// [length: 8 bytes][timestamp: 8 bytes][checksum: 4 bytes][data: <length> bytes]
    /// ```
    /// Immediately flushes the buffer.
    fn write_record(
        current_journal: &mut File,
        data: Vec<u8>,
    ) -> Result<JournalHandle, NornsDbError> {
        let timestamp = current_timestamp();

        let data_length = data.len() as u64;

        let checksum = crc_fast::crc32_iscsi(&data);

        trace!(%data_length, checksum, "writing journal record");

        current_journal.write_all(&data_length.to_le_bytes())?;
        current_journal.write_all(&timestamp.to_le_bytes())?;
        current_journal.write_all(&checksum.to_le_bytes())?;
        current_journal.write_all(&data)?;
        current_journal.flush()?;
        current_journal.sync_all()?;

        Ok(JournalHandle {
            offset: current_journal.stream_position()?,
        })
    }

    fn commit_journal(
        path: &Path,
        JournalHandle { offset }: JournalHandle,
    ) -> Result<(), NornsDbError> {
        Self::commit_journal_on_offset(path, offset)
    }

    fn commit_journal_on_offset(path: &Path, offset: u64) -> Result<(), NornsDbError> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path.with_extension("wal.commit"))?;

        file.write_all(&offset.to_le_bytes())?;
        file.flush()?;
        file.sync_all()?;

        Ok(())
    }
}

fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as _
}
