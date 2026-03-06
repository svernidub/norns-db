use std::time::{SystemTime, UNIX_EPOCH};

/// Tree-level WAL record
#[derive(Debug, bincode::Encode, bincode::Decode)]
pub struct JournalRecord<K, V> {
    /// Microsecond-precision timestamp
    timestamp: i64,

    kind: JournalRecordKind<K, V>,
}

#[derive(Debug, bincode::Encode, bincode::Decode)]
pub enum JournalRecordKind<K, V> {
    Upsert { key: K, value: V },

    Delete { key: K },
}

impl<K, V> JournalRecord<K, V> {
    pub fn new(kind: JournalRecordKind<K, V>) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;

        Self { timestamp, kind }
    }

    pub fn load(timestamp: i64, kind: JournalRecordKind<K, V>) -> Self {
        Self { timestamp, kind }
    }
}
