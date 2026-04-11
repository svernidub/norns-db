use super::{frozen_memtables::FrozenMemtable, value::Value};
use std::{collections::BTreeMap, sync::Arc};

pub(super) enum ActiveMemtable<K, V, H> {
    Empty,
    NonEmpty {
        data: BTreeMap<K, Value<V>>,
        recent_handle: H,
    },
}

impl<K: Ord, V, H> ActiveMemtable<K, V, H> {
    pub fn insert(&mut self, key: K, value: Value<V>, handle: H) {
        match self {
            Self::Empty => {
                *self = Self::NonEmpty {
                    data: BTreeMap::from([(key, value)]),
                    recent_handle: handle,
                };
            }
            Self::NonEmpty {
                data,
                recent_handle,
            } => {
                data.insert(key, value);
                *recent_handle = handle;
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::NonEmpty { data, .. } => data.len(),
        }
    }

    pub fn get(&self, key: &K) -> Option<&Value<V>> {
        match self {
            Self::Empty => None,
            Self::NonEmpty { data, .. } => data.get(key),
        }
    }

    /// Takes the contents, leaving `Empty`. Returns `None` if already empty.
    pub fn freeze(&mut self) -> Option<Arc<FrozenMemtable<K, V, H>>> {
        match std::mem::replace(self, Self::Empty) {
            Self::Empty => None,
            Self::NonEmpty {
                data,
                recent_handle,
            } => Some(Arc::new(FrozenMemtable {
                data,
                recent_handle,
            })),
        }
    }
}
