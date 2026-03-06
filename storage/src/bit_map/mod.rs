#[cfg(test)]
mod tests;

macro_rules! debug_assert_index {
    ($length:expr, $index:expr) => {
        debug_assert!(
            $index < $length,
            "index {} out of bounds for BitMap of size {}",
            $index,
            $length
        );
    };
}

#[derive(Debug, bincode::Encode, bincode::Decode)]
pub struct BitMap {
    map: Vec<u64>,
    bit_size: usize,
}

impl BitMap {
    pub fn new(size: usize) -> Self {
        let (mut bytes, rem) = (size / 64, size % 64);
        if rem > 0 {
            bytes += 1;
        }

        Self {
            map: vec![0; bytes],
            bit_size: size,
        }
    }

    #[inline]
    pub fn is_set(&self, idx: usize) -> bool {
        debug_assert_index!(self.bit_size, idx);

        let (byte_idx, mask) = self.get_byte_index_and_mask(idx);
        let byte = self.map[byte_idx];

        byte & mask != 0
    }

    #[inline]
    pub fn set(&mut self, idx: usize) {
        debug_assert_index!(self.bit_size, idx);

        let (byte_idx, mask) = self.get_byte_index_and_mask(idx);
        let byte = self.map[byte_idx];

        let new_byte = mask | byte;

        self.map[byte_idx] = new_byte;
    }

    #[inline]
    pub fn reset(&mut self, idx: usize) {
        debug_assert_index!(self.bit_size, idx);

        let (byte_idx, mask) = self.get_byte_index_and_mask(idx);
        let byte = self.map[byte_idx];

        let new_byte = byte & !mask;

        self.map[byte_idx] = new_byte;
    }

    #[inline]
    pub fn bit_size(&self) -> usize {
        self.bit_size
    }

    #[inline]
    pub fn byte_size(&self) -> usize {
        self.map.len()
    }

    #[inline]
    fn get_byte_index_and_mask(&self, idx: usize) -> (usize, u64) {
        let bit_in_byte = (idx % 64) as u64;
        let mask = 1u64 << bit_in_byte;

        (idx / 64, mask)
    }
}
