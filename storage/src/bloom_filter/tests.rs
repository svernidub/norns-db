use crate::bloom_filter::BloomFilter;
use std::collections::BTreeSet;

#[test]
fn test_filter_size() {
    let filter = BloomFilter::<u64>::new(100000, 0.2);

    assert_eq!(filter.filter.byte_size(), 5235);
    assert_eq!(filter.hash_functions, 3);

    let filter = BloomFilter::<u64>::new(10000, 0.2);

    assert_eq!(filter.filter.byte_size(), 524);
    assert_eq!(filter.hash_functions, 3);

    let filter = BloomFilter::<u64>::new(100000, 0.1);

    assert_eq!(filter.filter.byte_size(), 7489);
    assert_eq!(filter.hash_functions, 4);

    let filter = BloomFilter::<u64>::new(10000, 0.1);

    assert_eq!(filter.filter.byte_size(), 749);
    assert_eq!(filter.hash_functions, 4);
}

#[test]
fn test_addition_and_finding() {
    let false_positives_probability = 0.1;
    let capacity = 10_000;

    let mut filter = BloomFilter::new(capacity, false_positives_probability);

    let values: BTreeSet<u64> = (0..capacity as u64).collect();

    for i in &values {
        filter.add(i);
    }

    for &i in &values {
        assert!(filter.contains(&i), "false negative for {i}");
    }

    let probe_count = 100_000u64;
    let mut false_positive = 0;

    for i in capacity as u64..(capacity as u64 + probe_count) {
        if filter.contains(&i) {
            false_positive += 1;
        }
    }

    let ratio = false_positive as f64 / probe_count as f64;
    let ratio_diff = (ratio - false_positives_probability).abs();

    assert!(
        ratio_diff < 0.01,
        "FP ratio {ratio} too far from target {false_positives_probability}"
    );
}
