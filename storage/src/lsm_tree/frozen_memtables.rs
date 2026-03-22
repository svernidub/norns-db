use super::value::Value;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Condvar, Mutex},
};

pub type FrozenMemtables<K, V> = Arc<(Mutex<VecDeque<Arc<BTreeMap<K, Value<V>>>>>, Condvar)>;
