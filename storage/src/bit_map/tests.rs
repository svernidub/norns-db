use crate::bit_map::BitMap;

#[test]
fn test_put_some_values() {
    let mut bit_map = BitMap::new(12);

    for i in 0..12 {
        bit_map.set(i);
    }

    for i in 0..12 {
        assert!(bit_map.is_set(i));
    }

    for i in 0..12 {
        bit_map.reset(i);
    }

    for i in 0..12 {
        assert!(!bit_map.is_set(i));
    }
}

#[test]
fn test_size_not_multiple_of_64() {
    let bit_map = BitMap::new(12);
    assert_eq!(bit_map.map.len(), 1);

    let bit_map = BitMap::new(800);
    assert_eq!(bit_map.map.len(), 13);
}
