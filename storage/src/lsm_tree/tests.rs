use crate::lsm_tree::LsmTree;
use std::path::Path;

#[test]
fn test_simple_insert_and_get() {
    let tree = lsm_three("simple_insert_and_get");

    let key = "Hello".to_string();
    let value = "World".to_string();

    tree.insert(key.clone(), value.clone(), ()).unwrap();

    let found_value = tree.get(&key).unwrap();

    assert_eq!(found_value, Some(value));
}

#[test]
fn test_memtable_never_exceeds_configured_size_while_all_data_is_accessible() {
    let tree = lsm_three("memtable_never_exceeds_configured_size_while_all_data_is_accessible");

    for i in 0..=1000 {
        tree.insert(format!("key_{i}"), format!("value_{i}"), ())
            .unwrap();
    }

    assert_eq!(tree.memtable.read().unwrap().len(), 1);

    tree.flush_sync().unwrap();

    for i in 0..=1000 {
        let value = tree.get(&format!("key_{i}")).unwrap();
        let expected_value = format!("value_{i}");
        assert_eq!(value, Some(expected_value));
    }
}

#[test]
fn test_finds_key_across_multiple_ss_tables() {
    let tree = lsm_three("finds_key_across_multiple_ss_tables");

    for i in 0..800 {
        tree.insert(format!("key_{i}"), format!("value_{i}"), ())
            .unwrap();
    }

    tree.flush_sync().unwrap();

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
fn test_load() {
    let path = test_dir("load_tree");

    {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let tree = LsmTree::new("load_tree".to_string(), path.clone(), 100, 10, 10, 2, tx).unwrap();

        for i in 0..800 {
            tree.insert(format!("key_{i}"), format!("value_{i}"), ())
                .unwrap();
        }

        tree.flush_sync().unwrap();
    }

    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let tree = LsmTree::<String, String, ()>::load("load_tree".to_string(), path, 2, tx).unwrap();

    let value = tree.get(&"key_12".to_string()).unwrap();

    assert_eq!(value, Some("value_12".to_string()));
}

#[test]
fn test_compaction_moves_data_to_level_1() {
    let path = test_dir("test_compaction_moves_data_to_level_1");
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let tree = LsmTree::new(
        "test_compaction_moves_data_to_level_1".to_string(),
        path.clone(),
        100,
        5,
        10,
        2,
        tx,
    )
    .unwrap();

    // Produces 5 saved SS Tables; with level_0_size=5, the 5th auto-flush triggers compaction
    for i in 0..500 {
        tree.insert(format!("key_{i}"), format!("value_{i}"), ())
            .unwrap();
    }

    tree.flush_sync().unwrap();
    tree.compact_sync().unwrap();

    assert_eq!(
        std::fs::read_dir(Path::new(&path).join("level0"))
            .unwrap()
            .count(),
        0
    );

    assert_eq!(
        std::fs::read_dir(Path::new(&path).join("level1"))
            .unwrap()
            .count(),
        3 // for idx, filter and data
    );
}

#[test]
fn test_after_compaction_data_is_still_accessible() {
    let path = test_dir("test_after_compaction_data_is_still_accessible");
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let tree = LsmTree::new(
        "test_after_compaction_data_is_still_accessible".to_string(),
        path,
        100,
        5,
        10,
        2,
        tx,
    )
    .unwrap();

    for i in 0..500 {
        tree.insert(format!("key_{i}"), format!("value_{i}"), ())
            .unwrap();
    }

    tree.flush_sync().unwrap();
    tree.compact_sync().unwrap();

    let value = tree.get(&"key_18".to_string()).unwrap();
    assert_eq!(value, Some("value_18".to_string()));
}

#[test]
fn test_compaction_leaves_more_recent_key_value() {
    let path = test_dir("test_compaction_leaves_more_recent_key_value");
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let tree = LsmTree::new(
        "test_compaction_leaves_more_recent_key_value".to_string(),
        path,
        100,
        5,
        10,
        2,
        tx,
    )
    .unwrap();

    // 5 explicit flush_sync calls fill L0; with level_0_size=5 the 5th triggers auto-compaction
    for i in 0..5 {
        tree.insert("key".to_string(), format!("v{i}"), ()).unwrap();
        tree.flush_sync().unwrap();
    }

    tree.compact_sync().unwrap();

    let value = tree.get(&"key".to_string()).unwrap();

    assert_eq!(value, Some("v4".to_string()));
}

#[test]
fn test_delete_if_key_exists() {
    let tree = lsm_three("test_delete_if_key_exists");

    for i in 0..1500 {
        tree.insert(format!("key_{i}"), format!("value_{i}"), ())
            .unwrap();
    }

    let value = tree.get(&"key_12".to_string()).unwrap();
    assert_eq!(value, Some("value_12".to_string()));

    tree.delete("key_12".to_string(), ()).unwrap();

    let value = tree.get(&"key_12".to_string()).unwrap();
    assert_eq!(value, None);
}

#[test]
fn test_delete_when_key_does_not_exist() {
    let tree = lsm_three("test_delete_when_key_does_not_exist");

    for i in 0..1500 {
        tree.insert(format!("key_{i}"), format!("value_{i}"), ())
            .unwrap();
    }

    tree.delete("not_exists".to_string(), ()).unwrap();

    let value = tree.get(&"not_exists".to_string()).unwrap();
    assert!(value.is_none());
}

#[test]
fn test_compaction_works_with_deletion() {
    let tree = lsm_three("test_compaction_works_with_deletion");

    for i in 0..500 {
        tree.insert(format!("key_{i}"), format!("value_{i}"), ())
            .unwrap();
    }

    tree.insert("some_value".to_string(), "some_value".to_string(), ())
        .unwrap();

    for i in 500..1000 {
        tree.insert(format!("key_{i}"), format!("value_{i}"), ())
            .unwrap();
    }

    tree.delete("some_value".to_string(), ()).unwrap();

    for i in 1000..1500 {
        tree.insert(format!("key_{i}"), format!("value_{i}"), ())
            .unwrap();
    }

    tree.flush_sync().unwrap();
    tree.compact_sync().unwrap();

    assert!(tree.get(&"some_value".to_string()).unwrap().is_none());
}

#[test]
fn test_list_with_correct_order() {
    let path = test_dir("test_list_with_correct_order");
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let tree = LsmTree::new(
        "test_list_with_correct_order".to_string(),
        path,
        100,
        1,
        10,
        2,
        tx,
    )
    .unwrap();

    // let's write 10 keys with index=index, all will go to level1
    // and will add another element just to prove a reading from level1
    for i in 0..10 {
        tree.insert(i, i, ()).unwrap();
    }
    tree.insert(10, 10, ()).unwrap();
    tree.flush_sync().unwrap();
    tree.compact_sync().unwrap();

    // then every 2nd even item is multiplied by 2, all goes to level1 (level_0_size=1 auto-compacts)
    for i in (0..10).step_by(2) {
        tree.insert(i, i * 2, ()).unwrap();
    }
    tree.flush_sync().unwrap();

    // then every 2nd odd item is multiplied by 3, all goes to memtable
    for i in (1..10).step_by(2) {
        tree.insert(i, i * 3, ()).unwrap();
    }

    let items = tree.list().unwrap();

    let expected = vec![
        (0, 0),
        (1, 3),
        (2, 4),
        (3, 9),
        (4, 8),
        (5, 15),
        (6, 12),
        (7, 21),
        (8, 16),
        (9, 27),
        (10, 10),
    ];

    assert_eq!(items, expected);
}

#[test]
fn test_list_prefers_newer_value_across_level0_sstables_even_if_key_rank_differs() {
    let tree =
        lsm_three("list_prefers_newer_value_across_level0_sstables_even_if_key_rank_differs");

    // SSTable #0 (older): key "k" at rank 0 within the table
    tree.insert("k".to_string(), "old".to_string(), ()).unwrap();
    tree.flush_sync().unwrap();

    // SSTable #1 (newer): same key "k" but at a later rank (after a,b,c)
    tree.insert("a".to_string(), "va".to_string(), ()).unwrap();
    tree.insert("b".to_string(), "vb".to_string(), ()).unwrap();
    tree.insert("c".to_string(), "vc".to_string(), ()).unwrap();
    tree.insert("k".to_string(), "new".to_string(), ()).unwrap();
    tree.flush_sync().unwrap();

    let items = tree.list().unwrap();
    let k_value = items.iter().find(|(k, _)| k == "k").map(|(_, v)| v.clone());
    assert_eq!(k_value, Some("new".to_string()));
}

#[test]
fn test_no_data_loss_when_flush_and_compaction_overlap() {
    // memtable_size=10, level_0_size=3: every 10 inserts → flush; every 3 flushes → compaction.
    // Writing 500 keys crosses the L0 threshold multiple times, exercising the concurrent
    // flush+compaction path that previously caused a lost-update data race.
    let path = test_dir("flush_compaction_overlap");
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let tree = LsmTree::new(
        "flush_compaction_overlap".to_string(),
        path,
        10,
        3,
        10,
        5,
        tx,
    )
    .unwrap();

    for i in 0..500 {
        tree.insert(format!("key_{i}"), format!("value_{i}"), ())
            .unwrap();
    }

    tree.flush_sync().unwrap();
    tree.compact_sync().unwrap();

    for i in 0..500 {
        assert_eq!(
            tree.get(&format!("key_{i}")).unwrap(),
            Some(format!("value_{i}")),
            "key_{i} lost after flush+compaction"
        );
    }
}

#[test]
fn test_commit_handle_sent_after_flush_sync() {
    let path = test_dir("commit_handle_sent_after_flush_sync");
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let tree = LsmTree::new(
        "commit_handle_sent_after_flush_sync".to_string(),
        path,
        100,
        10,
        10,
        2,
        tx,
    )
    .unwrap();

    for i in 0..5 {
        tree.insert(format!("key_{i}"), format!("value_{i}"), ())
            .unwrap();
    }

    tree.flush_sync().unwrap();

    let mut commit_count = 0;
    while rx.try_recv().is_ok() {
        commit_count += 1;
    }
    assert_eq!(commit_count, 1);
}

#[test]
fn test_commit_handle_count_matches_frozen_memtable_count() {
    let path = test_dir("commit_handle_count_matches_frozen_memtable_count");
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let tree = LsmTree::new(
        "commit_handle_count_matches_frozen_memtable_count".to_string(),
        path,
        10, // memtable_size — 20 inserts create exactly 2 frozen memtables
        10,
        10,
        5, // max_frozen_memtables — large enough not to block
        tx,
    )
    .unwrap();

    for i in 0..20 {
        tree.insert(format!("key_{i}"), format!("value_{i}"), ())
            .unwrap();
    }

    tree.flush_sync().unwrap();

    let mut commit_count = 0;
    while rx.try_recv().is_ok() {
        commit_count += 1;
    }
    assert_eq!(commit_count, 2);
}

#[test]
fn test_no_commit_handle_when_nothing_is_flushed() {
    let path = test_dir("no_commit_handle_when_nothing_is_flushed");
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let tree = LsmTree::<String, String, ()>::new(
        "no_commit_handle_when_nothing_is_flushed".to_string(),
        path,
        100,
        10,
        10,
        2,
        tx,
    )
    .unwrap();

    tree.flush_sync().unwrap();

    assert!(rx.try_recv().is_err());
}

fn lsm_three(test_name: &str) -> LsmTree<String, String, ()> {
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let path = test_dir(test_name);
    LsmTree::new(test_name.to_string(), path, 100, 10, 10, 2, tx).unwrap()
}

fn test_dir(name: &str) -> String {
    let dir = std::env::temp_dir()
        .join("norns_db_lsm_tree_tests")
        .join(name);

    let _ = std::fs::remove_dir_all(&dir);
    dir.to_str().unwrap().to_string()
}
