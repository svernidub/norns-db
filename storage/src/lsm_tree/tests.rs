use crate::lsm_tree::LsmTree;

fn test_dir(name: &str) -> String {
    let dir = std::env::temp_dir()
        .join("norns_db_lsm_tree_tests")
        .join(name);
    let _ = std::fs::remove_dir_all(&dir);
    dir.to_str().unwrap().to_string()
}

#[test]
fn test_simple_insert_and_get() {
    let mut tree = lsm_three("test_simple_insert_and_get");

    let key = "Hello".to_string();
    let value = "World".to_string();

    tree.insert(key.clone(), value.clone()).unwrap();

    let found_value = tree.get(&key).unwrap();

    assert_eq!(found_value, Some(value));
}

#[test]
fn test_memtable_never_exceeds_configured_size_while_all_data_is_accessible() {
    let mut tree =
        lsm_three("test_memtable_never_exceeds_configured_size_while_all_data_is_accessible");

    for i in 0..1000 {
        tree.insert(format!("key_{i}"), format!("value_{i}"))
            .unwrap();
    }

    assert_eq!(tree.map.len(), 100);

    for i in 0..1000 {
        let value = tree.get(&format!("key_{i}")).unwrap();
        let expected_value = format!("value_{i}");
        assert_eq!(value, Some(expected_value));
    }
}

#[test]
fn test_finds_key_across_multiple_ss_tables() {
    let path = test_dir("test_finds_key_across_multiple_ss_tables");
    let mut tree = LsmTree::new(path.clone(), 100, 10, 10).unwrap();

    for i in 0..800 {
        tree.insert(format!("key_{i}"), format!("value_{i}"))
            .unwrap();
    }

    tree.flush().unwrap();

    // key_18 lives in the oldest SSTable (#0). The search goes in reverse chronological
    // order (newest first), so bloom filters on SSTables #7..#1 must correctly reject
    // the key before it's found in #0.
    let key = "key_18".to_string();
    assert_eq!(tree.get(&key).unwrap(), Some("value_18".to_string()));

    // A key that was never inserted should not be found in any SSTable.
    let missing = "key_99999".to_string();
    assert!(tree.get(&missing).unwrap().is_none());
}

#[test]
fn load_tree() {
    let path = test_dir("load_tree");
    let mut tree = LsmTree::new(path.clone(), 100, 10, 10).unwrap();

    for i in 0..800 {
        tree.insert(format!("key_{i}"), format!("value_{i}"))
            .unwrap();
    }

    tree.flush().unwrap();

    let tree = LsmTree::<String, String>::load(path).unwrap();

    let value = tree.get(&"key_12".to_string()).unwrap();

    assert_eq!(value, Some("value_12".to_string()));
}

#[test]
fn test_compaction_moves_data_to_level_1() {
    let path = test_dir("test_compaction_moves_data_to_level_1");
    let mut tree = LsmTree::new(path.clone(), 100, 10, 10).unwrap();

    // Produces 5 saved SS Tables, so 15 files
    for i in 0..500 {
        tree.insert(format!("key_{i}"), format!("value_{i}"))
            .unwrap();
    }
    tree.flush().unwrap();

    tree.compact().unwrap();

    assert_eq!(
        std::fs::read_dir(format!("{path}/level0"))
            .unwrap()
            .count(),
        0
    );

    assert_eq!(
        std::fs::read_dir(format!("{path}/level1"))
            .unwrap()
            .count(),
        3 // for idx, filter and data
    );
}

#[test]
fn test_after_compaction_data_is_still_accessible() {
    let mut tree = lsm_three("test_after_compaction_data_is_still_accessible");

    for i in 0..500 {
        tree.insert(format!("key_{i}"), format!("value_{i}"))
            .unwrap();
    }
    tree.flush().unwrap();
    tree.compact().unwrap();

    let value = tree.get(&"key_18".to_string()).unwrap();
    assert_eq!(value, Some("value_18".to_string()));
}

#[test]
fn test_compaction_leaves_more_recent_key_value() {
    let mut tree = lsm_three("test_compaction_leaves_more_recent_key_value");

    for i in 0..5 {
        tree.insert("key".to_string(), format!("v{i}")).unwrap();
        tree.flush().unwrap();
    }

    tree.compact().unwrap();

    let value = tree.get(&"key".to_string()).unwrap();

    assert_eq!(value, Some("v4".to_string()));
}

#[test]
fn test_delete_if_key_exists() {
    let mut tree = lsm_three("test_delete_if_key_exists");

    for i in 0..1500 {
        tree.insert(format!("key_{i}"), format!("value_{i}"))
            .unwrap();
    }

    let value = tree.get(&"key_12".to_string()).unwrap();
    assert_eq!(value, Some("value_12".to_string()));

    let value = tree.delete("key_12".to_string()).unwrap();
    assert_eq!(value, Some("value_12".to_string()));

    let value = tree.get(&"key_12".to_string()).unwrap();
    assert_eq!(value, None);
}

#[test]
fn test_skip_if_key_does_not_exist() {
    let mut tree = lsm_three("test_skip_if_key_does_not_exist");

    for i in 0..1500 {
        tree.insert(format!("key_{i}"), format!("value_{i}"))
            .unwrap();
    }

    let value = tree.get(&"not_exists".to_string()).unwrap();
    assert!(value.is_none());

    let value = tree.delete("not_exists".to_string()).unwrap();
    assert!(value.is_none());
}

#[test]
fn test_compaction_works_with_deletion() {
    let mut tree = lsm_three("test_compaction_works_with_deletion");

    for i in 0..500 {
        tree.insert(format!("key_{i}"), format!("value_{i}"))
            .unwrap();
    }

    tree.insert("some_value".to_string(), "some_value".to_string())
        .unwrap();

    for i in 500..1000 {
        tree.insert(format!("key_{i}"), format!("value_{i}"))
            .unwrap();
    }

    tree.delete("some_value".to_string()).unwrap();

    for i in 1000..1500 {
        tree.insert(format!("key_{i}"), format!("value_{i}"))
            .unwrap();
    }

    tree.flush().unwrap();

    tree.compact().unwrap();

    assert!(tree.get(&"some_value".to_string()).unwrap().is_none());
}

fn lsm_three(test_name: &str) -> LsmTree<String, String> {
    let path = test_dir(test_name);
    LsmTree::new(path, 100, 10, 10).unwrap()
}
