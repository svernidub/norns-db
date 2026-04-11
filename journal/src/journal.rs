use crate::{JournalHandle, JournalRecordData, writer_worker::WriterWorker};
use dbcore::error::NornsDbError;
use std::{
    fs::OpenOptions,
    marker::PhantomData,
    path::{Path, PathBuf},
};
use tracing::*;

pub struct Journal<K, V> {
    writer: WriterWorker,
    #[allow(dead_code)]
    path: PathBuf,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> Journal<K, V> {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, NornsDbError> {
        let path = path.as_ref().to_path_buf();

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .inspect_err(|e| {
                error!(path = %path.display(), error = %e, "failed to open journal file");
            })?;

        let writer = WriterWorker::new(file, path.clone());

        debug!(path = %path.display(), "journal opened");

        Ok(Self {
            writer,
            path,
            _marker: PhantomData,
        })
    }

    pub async fn append(
        &self,
        record: JournalRecordData<K, V>,
    ) -> Result<JournalHandle, NornsDbError>
    where
        K: bincode::Encode,
        V: bincode::Encode,
    {
        let body =
            bincode::encode_to_vec(record, bincode::config::standard()).inspect_err(|e| {
                error!(error = %e, "failed to encode journal record");
            })?;

        debug!(body_size = body.len(), "appending journal record");

        self.writer.insert_entry(body).await
    }

    pub async fn commit(&self, handle: JournalHandle) -> Result<(), NornsDbError> {
        self.writer.commit(handle).await
    }

    pub async fn shutdown(self) -> Result<(), NornsDbError> {
        self.writer.shutdown().await
    }
}
