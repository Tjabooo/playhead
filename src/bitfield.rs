//! Piece bitfield (BEP 3 layout: piece 0 is the high bit of byte 0).

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Bitfield {
    bits: Vec<u8>,
    len: u32,
    count: u32,
}

impl Bitfield {
    pub fn new(len: u32) -> Self {
        Bitfield { bits: vec![0; len.div_ceil(8) as usize], len, count: 0 }
    }

    pub fn full(len: u32) -> Self {
        let mut b = Self::new(len);
        for i in 0..len {
            b.set(i);
        }
        b
    }

    /// From wire bytes; spare bits must be zero.
    pub fn from_bytes(bytes: &[u8], len: u32) -> Option<Self> {
        if bytes.len() != len.div_ceil(8) as usize {
            return None;
        }
        let mut b = Bitfield { bits: bytes.to_vec(), len, count: 0 };
        for i in len..(bytes.len() as u32 * 8) {
            if b.raw_get(i) {
                return None;
            }
        }
        b.count = (0..len).filter(|&i| b.raw_get(i)).count() as u32;
        Some(b)
    }

    fn raw_get(&self, i: u32) -> bool {
        self.bits[(i / 8) as usize] & (0x80 >> (i % 8)) != 0
    }

    pub fn get(&self, i: u32) -> bool {
        i < self.len && self.raw_get(i)
    }

    pub fn set(&mut self, i: u32) {
        if i < self.len && !self.raw_get(i) {
            self.bits[(i / 8) as usize] |= 0x80 >> (i % 8);
            self.count += 1;
        }
    }

    pub fn clear(&mut self, i: u32) {
        if i < self.len && self.raw_get(i) {
            self.bits[(i / 8) as usize] &= !(0x80 >> (i % 8));
            self.count -= 1;
        }
    }

    pub fn len(&self) -> u32 {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn count(&self) -> u32 {
        self.count
    }
    pub fn is_full(&self) -> bool {
        self.count == self.len
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.bits
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout() {
        let mut b = Bitfield::new(10);
        b.set(0);
        b.set(9);
        b.set(9);
        assert_eq!(b.as_bytes(), &[0x80, 0x40]);
        assert_eq!(b.count(), 2);
        assert!(Bitfield::from_bytes(&[0x80, 0x41], 10).is_none());
        assert_eq!(Bitfield::from_bytes(&[0xff, 0xc0], 10).unwrap().count(), 10);
    }
}
