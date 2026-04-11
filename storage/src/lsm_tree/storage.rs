use super::value::Value;
use crate::ss_table::SsTable;
use std::sync::Arc;

pub struct Storage<K, V> {
    pub level_0_ss_tables: Vec<Arc<SsTable<K, Value<V>>>>,
    pub level_1_ss_tables: Vec<Arc<SsTable<K, Value<V>>>>,
}
