use crate::ss_table::SsTable;
use std::{collections::BTreeMap, path::PathBuf};

fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join("norns_db_ss_table_tests")
        .join(name);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn test_existing_key_search() {
    let table = ss_table("test_existing_key_search");
    let key = "key_500".to_string();
    assert_eq!(table.get(&key).unwrap().unwrap(), "value_500");
}

#[test]
fn test_does_not_find_missing_key() {
    let table = ss_table("test_does_not_find_missing_key");
    let key = "key_50000".to_string();
    assert!(table.get(&key).unwrap().is_none());
}

#[test]
fn finds_first_key_in_index() {
    let table = ss_table("finds_first_key_in_index");

    let key = "key_0".to_string();

    assert_eq!(table.get(&key).unwrap().unwrap(), "value_0");
}

#[test]
fn test_finds_key_after_last_index_item() {
    let table = ss_table("test_finds_key_after_last_index_item");

    let key = "key_9999".to_string(); // Should be for sure one of the lasts

    assert!(!table.block_index.contains_key(&key));

    assert_eq!(table.get(&key).unwrap().unwrap(), "value_9999");
}

#[test]
fn test_missing_key_is_not_searched_in_db() {
    let table = ss_table("test_missing_key_is_not_searched_in_db");
    let data_path = test_dir("test_missing_key_is_not_searched_in_db").join("table.data");
    std::fs::remove_file(data_path).unwrap();

    // It's the matter of probability that we will not receive a false
    // positive from bloom filter, so a radom missing key is chosen that was
    // not found in bloom filter.
    let key = "key_500000".to_string();

    assert!(!table.bloom_filter.contains(&key));

    assert!(table.get(&key).unwrap().is_none());
}

#[test]
fn test_iterator_consumption() {
    let table = ss_table("test_iterator");

    let map: BTreeMap<_, _> = table.iter().unwrap().collect::<Result<_, _>>().unwrap();

    let key = "key_500".to_string();
    assert_eq!(map.get(&key).unwrap(), "value_500");
}

#[test]
fn test_finds_last_key_in_block() {
    let table = ss_table("test_finds_last_key_in_block");

    // Collect sorted keys to find the last key in the first block (block_size=10)
    let mut keys: Vec<_> = (0..10000).map(|i| format!("key_{i}")).collect();
    keys.sort();

    let last_in_block = &keys[9];

    assert_eq!(
        table.get(last_in_block).unwrap().unwrap(),
        format!("value_{}", last_in_block.strip_prefix("key_").unwrap())
    );
}

fn ss_table(name: &str) -> SsTable<String, String> {
    let mut data = BTreeMap::new();

    for i in 0..10000 {
        data.insert(format!("key_{i}"), format!("value_{i}"));
    }

    let path = test_dir(name).join("table");
    SsTable::<String, String>::new(data, &path, 10).unwrap();
    SsTable::<String, String>::load(&path).unwrap()
}
