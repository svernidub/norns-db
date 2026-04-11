/// Tree-level WAL record data
#[derive(Debug, bincode::Encode, bincode::Decode)]
pub enum JournalRecordData<K, V> {
    Upsert { key: K, value: V },

    Delete { key: K },
}
