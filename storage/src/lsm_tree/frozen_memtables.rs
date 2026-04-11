use super::value::Value;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Condvar, Mutex},
};

pub(super) struct FrozenMemtable<K, V, H> {
    pub data: BTreeMap<K, Value<V>>,
    pub recent_handle: H,
}

// We use double Arc here for fast clones on flushes. The data will be flushed, while
// the queue is accessible for read
pub(super) type FrozenMemtables<K, V, H> =
    Arc<(Mutex<VecDeque<Arc<FrozenMemtable<K, V, H>>>>, Condvar)>;
