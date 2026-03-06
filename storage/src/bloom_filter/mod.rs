#[cfg(test)]
mod tests;

use crate::bit_map::BitMap;
use siphasher::sip::SipHasher;
use std::{
    hash::{Hash, Hasher},
    marker::PhantomData,
};

/// Probabilistic structure that allows to identify if we have probably met some element of a set
/// before.
///
/// If the filter negatively responds to contains, we can be 100% sure that the value was not added
/// to the filter. You'll get the expected level of false positives only if you use all requested
/// capacity, otherwise you'll may have even smaller values.
///
/// You must configure the planned capacity of elements and the desired number of false-positives.
/// Both values determine the size of the underlying data structure. If we need to put more values
/// to the filter or receive false-positive less probably, then the underlying structure will be
/// bigger.
///
/// The real underlying data structure size depends on planned capacity but may be less or even
/// bigger than planned capacity.
#[derive(Debug, bincode::Encode, bincode::Decode)]
pub struct BloomFilter<T> {
    filter: BitMap,
    hash_functions: usize,
    _phantom: PhantomData<T>,
}

impl<T> BloomFilter<T> {
    /// Creates a new filter with new configured capacity and false positives' probability.
    pub fn new(planned_capacity: usize, false_positives_probability: f64) -> Self {
        debug_assert!(false_positives_probability > 0.0);
        debug_assert!(false_positives_probability < 1.0);
        debug_assert!(planned_capacity > 0);

        let planned_capacity = planned_capacity as f64;

        let bits = (-planned_capacity * false_positives_probability.ln()) / 2_f64.ln().powf(2.0);

        let hash_functions = (bits / planned_capacity * 2_f64.ln()).ceil() as usize;

        Self {
            filter: BitMap::new(bits as usize),
            hash_functions,
            _phantom: Default::default(),
        }
    }
}

impl<T> BloomFilter<T>
where
    T: Hash,
{
    pub fn add(&mut self, item: &T) {
        let bit_indexes =
            Self::get_bit_index_iter(self.hash_functions, self.filter.bit_size(), item);

        for bit_index in bit_indexes {
            self.filter.set(bit_index);
        }
    }

    pub fn contains(&self, item: &T) -> bool {
        let bit_indexes =
            Self::get_bit_index_iter(self.hash_functions, self.filter.bit_size(), item);

        for bit_index in bit_indexes {
            if !self.filter.is_set(bit_index) {
                return false;
            }
        }

        true
    }

    fn get_bit_index_iter(
        functions: usize,
        filter_len: usize,
        item: &T,
    ) -> impl Iterator<Item = usize> {
        (0..functions).map(move |i| {
            let mut hasher = SipHasher::new();

            item.hash(&mut hasher);
            i.hash(&mut hasher);

            hasher.finish() as usize % filter_len
        })
    }
}
