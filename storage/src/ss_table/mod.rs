#[cfg(test)]
mod tests;

use crate::bloom_filter::BloomFilter;
use std::{
    collections::BTreeMap,
    error::Error,
    fs::File,
    hash::Hash,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
};

pub struct SsTable<K, V> {
    bloom_filter: BloomFilter<K>,
    block_index: BTreeMap<K, u64>,
    table_data_path: String,
    _marker: PhantomData<V>,
}

pub struct SsTableIter<K, V> {
    reader: BufReader<File>,
    phantom_data: PhantomData<(K, V)>,
}

impl<K, V> Iterator for SsTableIter<K, V>
where
    K: bincode::Decode<()>,
    V: bincode::Decode<()>,
{
    type Item = Result<(K, V), Box<dyn Error>>;

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
            Err(e) => Some(Err(Box::new(e))),
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
        table_path: &str,
        block_size: usize,
    ) -> Result<Self, Box<dyn Error>> {
        debug_assert!(data.len() > 0);

        let mut data_writer = BufWriter::new(File::create(format!("{table_path}.data"))?);

        let mut bloom_filter = BloomFilter::new(data.len(), 0.1);
        let mut block_index: BTreeMap<K, u64> = BTreeMap::new();

        let mut block = Vec::new();
        let mut block_index_key = None;

        for (i, (key, value)) in data.into_iter().enumerate() {
            bloom_filter.add(&key);

            if block_index_key.is_none() {
                block_index_key = Some(key.clone());
            }

            let data = bincode::encode_to_vec(&(key, value), bincode::config::standard())?;
            block.extend(&data);

            if (i + 1) % block_size == 0 {
                let pos = data_writer.stream_position()?;
                block_index.insert(block_index_key.unwrap(), pos);

                data_writer.write_all(&block)?;
                block.clear();

                block_index_key = None;
            };
        }

        if !block.is_empty() {
            let pos = data_writer.stream_position()?;
            block_index.insert(block_index_key.unwrap(), pos);

            data_writer.write_all(&block)?;
            block.clear();
        }

        data_writer.flush()?;

        Self::serialize_on_disk(&block_index, format!("{table_path}.idx"))?;
        Self::serialize_on_disk(&bloom_filter, format!("{table_path}.bloom"))?;

        Ok(Self {
            bloom_filter,
            block_index,
            table_data_path: format!("{table_path}.data"),
            _marker: Default::default(),
        })
    }

    pub fn load(table_path: String) -> Result<Self, Box<dyn Error>> {
        let block_index = {
            let mut index_reader = BufReader::new(File::open(format!("{table_path}.idx"))?);
            let mut index_buf = Vec::new();
            index_reader.read_to_end(&mut index_buf)?;

            let (block_index, _) =
                bincode::decode_from_slice(&index_buf, bincode::config::standard())?;

            block_index
        };

        let bloom_filter = {
            let mut bloom_reader = BufReader::new(File::open(format!("{table_path}.bloom"))?);
            let mut index_buf = Vec::new();
            bloom_reader.read_to_end(&mut index_buf)?;

            let (bloom_filter, _) =
                bincode::decode_from_slice(&index_buf, bincode::config::standard())?;

            bloom_filter
        };

        Ok(Self {
            bloom_filter,
            block_index,
            table_data_path: format!("{table_path}.data"),
            _marker: Default::default(),
        })
    }

    pub fn get(&self, key: &K) -> Result<Option<V>, Box<dyn Error>> {
        if !self.bloom_filter.contains(key) {
            return Ok(None);
        }

        let Some((index_key, pos)) = self.block_index.range(..=key.to_owned()).next_back() else {
            return Ok(None); // index does not cover the value? Maybe it's empty...
        };

        let stop_search = self
            .block_index
            .range(index_key.to_owned()..)
            .nth(1)
            .map(|(_, pos)| *pos);

        let mut data_reader = BufReader::new(File::open(&self.table_data_path)?);

        data_reader.seek(SeekFrom::Start(*pos))?;

        while stop_search.is_none() || Some(data_reader.stream_position()?) <= stop_search {
            let result = bincode::decode_from_reader::<(K, V), _, _>(
                &mut data_reader,
                bincode::config::standard(),
            );

            match result {
                Ok((pos_key, _)) if key != &pos_key => {
                    continue;
                }
                Ok((_, value)) => return Ok(Some(value)),
                Err(e) if matches!(&e, bincode::error::DecodeError::Io { inner, .. } if inner.kind() == std::io::ErrorKind::UnexpectedEof) =>
                {
                    return Ok(None);
                }
                Err(e) => return Err(Box::new(e)),
            }
        }

        Ok(None)
    }

    pub fn iter(&self) -> Result<SsTableIter<K, V>, Box<dyn Error>> {
        Ok(SsTableIter {
            reader: BufReader::new(File::open(&self.table_data_path)?),
            phantom_data: Default::default(),
        })
    }

    fn serialize_on_disk<D>(data: &D, file_name: String) -> Result<(), Box<dyn Error>>
    where
        D: bincode::Encode,
    {
        let serialized = bincode::encode_to_vec(data, bincode::config::standard())?;
        let mut writer = BufWriter::new(File::create(file_name)?);
        writer.write_all(&serialized)?;
        writer.flush()?;
        Ok(())
    }
}
