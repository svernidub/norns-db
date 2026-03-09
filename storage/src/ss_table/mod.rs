#[cfg(test)]
mod tests;

use crate::{bloom_filter::BloomFilter, io::read_at};
use dbcore::error::NornsDbError;
use std::{
    collections::BTreeMap,
    fs::File,
    hash::Hash,
    io::{BufReader, BufWriter, Read, Seek, Write},
    marker::PhantomData,
    path::Path,
};
use tracing::{debug, trace};

pub struct SsTable<K, V> {
    bloom_filter: BloomFilter<K>,
    block_index: BTreeMap<K, BlockIndexEntry>,
    data_file: File,
    _marker: PhantomData<V>,
}

pub struct SsTableIter<K, V> {
    reader: BufReader<File>,
    phantom_data: PhantomData<(K, V)>,
}

#[derive(bincode::Decode, bincode::Encode)]
struct BlockIndexEntry {
    offset: u64,
    length: u64,
}

impl<K, V> Iterator for SsTableIter<K, V>
where
    K: bincode::Decode<()>,
    V: bincode::Decode<()>,
{
    type Item = Result<(K, V), NornsDbError>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = bincode::decode_from_reader::<(K, V), _, _>(
            &mut self.reader,
            bincode::config::standard(),
        );

        match result {
            Ok((key, value)) => Some(Ok((key, value))),
            Err(e) if matches!(&e, bincode::error::DecodeError::Io { inner, .. } if inner.kind() == std::io::ErrorKind::UnexpectedEof) => {
                None
            }
            Err(e) => Some(Err(e.into())),
        }
    }
}

impl<K, V> SsTable<K, V>
where
    K: Hash + Clone + Ord + bincode::Encode + bincode::Decode<()>,
    V: bincode::Encode + bincode::Decode<()>,
{
    pub fn new(
        data: BTreeMap<K, V>,
        table_path: impl AsRef<Path>,
        block_size: usize,
    ) -> Result<Self, NornsDbError> {
        debug_assert!(!data.is_empty());

        let table_path = table_path.as_ref();
        let entries = data.len();
        debug!(
            path = %table_path.display(),
            entries,
            block_size,
            "creating new SSTable"
        );

        let data_file_name = table_path.with_extension("data");

        let mut data_writer = BufWriter::new(File::create(&data_file_name)?);

        let mut bloom_filter = BloomFilter::new(data.len(), 0.1);
        let mut block_index: BTreeMap<K, BlockIndexEntry> = BTreeMap::new();

        let mut block = Vec::new();
        let mut block_index_key = None;
        let mut length = 0u64;

        for (i, (key, value)) in data.into_iter().enumerate() {
            bloom_filter.add(&key);

            if block_index_key.is_none() {
                block_index_key = Some(key.clone());
            }

            let data = bincode::encode_to_vec(&(key, value), bincode::config::standard())?;
            block.extend(&data);
            length += data.len() as u64;

            if (i + 1) % block_size == 0 {
                let offset = data_writer.stream_position()?;
                block_index.insert(block_index_key.unwrap(), BlockIndexEntry { offset, length });

                data_writer.write_all(&block)?;
                block.clear();

                block_index_key = None;
                length = 0;
            };
        }

        if !block.is_empty() {
            let offset = data_writer.stream_position()?;
            block_index.insert(block_index_key.unwrap(), BlockIndexEntry { offset, length });
            data_writer.write_all(&block)?;
            block.clear();
        }

        data_writer.flush()?;
        data_writer.get_mut().sync_data()?;

        Self::serialize_on_disk(&block_index, table_path.with_extension("idx"))?;
        Self::serialize_on_disk(&bloom_filter, table_path.with_extension("bloom"))?;

        debug!(
            path = %table_path.display(),
            entries,
            blocks = block_index.len(),
            "SSTable created"
        );

        Ok(Self {
            bloom_filter,
            block_index,
            data_file: File::open(data_file_name)?,
            _marker: Default::default(),
        })
    }

    pub fn load(table_path: impl AsRef<Path>) -> Result<Self, NornsDbError> {
        let table_path = table_path.as_ref();
        debug!(path = %table_path.display(), "loading SSTable");

        let block_index = {
            let mut index_reader = BufReader::new(File::open(table_path.with_extension("idx"))?);
            let mut index_buf = Vec::new();
            index_reader.read_to_end(&mut index_buf)?;

            let (block_index, _) =
                bincode::decode_from_slice(&index_buf, bincode::config::standard())?;

            block_index
        };

        let bloom_filter = {
            let mut bloom_reader = BufReader::new(File::open(table_path.with_extension("bloom"))?);
            let mut index_buf = Vec::new();
            bloom_reader.read_to_end(&mut index_buf)?;

            let (bloom_filter, _) =
                bincode::decode_from_slice(&index_buf, bincode::config::standard())?;

            bloom_filter
        };

        debug!(path = %table_path.display(), "SSTable loaded");

        Ok(Self {
            bloom_filter,
            block_index,
            data_file: File::open(table_path.with_extension("data"))?,
            _marker: Default::default(),
        })
    }

    pub fn get(&self, key: &K) -> Result<Option<V>, NornsDbError> {
        if !self.bloom_filter.contains(key) {
            debug!("bloom filter rejected lookup");
            return Ok(None);
        }

        let Some((_index_key, &BlockIndexEntry { offset, length })) =
            self.block_index.range(..=key).next_back()
        else {
            debug!("key not found in block index");
            return Ok(None);
        };

        // Reading an entire block by index. The item *can* be somewhere in the middle of the block.
        trace!(offset, length, "reading block for key lookup");
        let mut buf = vec![0u8; length as usize];
        read_at(&self.data_file, &mut buf, offset)?;

        let mut cursor_offset = 0usize;

        while cursor_offset < buf.len() {
            let ((k, v), bytes_read) = bincode::decode_from_slice::<(K, V), _>(
                &buf[cursor_offset..],
                bincode::config::standard(),
            )?;

            if &k == key {
                trace!("key found in SSTable block");
                return Ok(Some(v));
            }

            cursor_offset += bytes_read;
        }

        Ok(None)
    }

    pub fn iter(&self) -> Result<SsTableIter<K, V>, NornsDbError> {
        Ok(SsTableIter {
            reader: BufReader::new(self.data_file.try_clone()?),
            phantom_data: Default::default(),
        })
    }

    fn serialize_on_disk<D>(data: &D, file_name: impl AsRef<Path>) -> Result<(), NornsDbError>
    where
        D: bincode::Encode,
    {
        let file_name = file_name.as_ref();
        trace!(path = %file_name.display(), "serializing to disk");
        let serialized = bincode::encode_to_vec(data, bincode::config::standard())?;
        let mut writer = BufWriter::new(File::create(file_name)?);
        writer.write_all(&serialized)?;
        writer.flush()?;
        writer.get_mut().sync_data()?;
        Ok(())
    }
}
