//! Hash Table
//!
//! Code taken from [The Algorithms - Rust](https://github.com/TheAlgorithms/Rust)

use std::vec::IntoIter;

pub struct HashTable<T> {
    buckets: Vec<Vec<(u64, T)>>,
    size: usize,
}

impl<T> HashTable<T> {
    #[inline(always)]
    pub fn new() -> Self {
        let initial_size = 1 << 16;
        let mut buckets = Vec::with_capacity(initial_size);

        for _ in 0..initial_size {
            buckets.push(Vec::new());
        }

        Self { buckets, size: 0 }
    }

    #[inline(always)]
    pub fn insert_or_update(
        &mut self,
        key: impl Hash,
        modify: impl FnOnce(&mut T),
        provide: impl FnOnce() -> T,
    ) {
        if self.size >= self.buckets.len() * 3 / 4 {
            self.resize();
        }
        let index = key.hash() as usize % self.buckets.len();
        if let Some(record) = self.buckets[index]
            .iter_mut()
            .find(|(k, _)| k.hash() == key.hash())
            .map(|(_, v)| v)
        {
            modify(record);
        } else {
            self.buckets[index].push((key.hash(), provide()));
            self.size += 1;
        }
    }

    #[inline(always)]
    fn resize(&mut self) {
        let new_size = self.buckets.len() * 2;
        let mut new_buckets = Vec::with_capacity(new_size);

        for _ in 0..new_size {
            new_buckets.push(Vec::new());
        }

        for bucket in self.buckets.drain(..) {
            for (key, value) in bucket {
                let index = key.hash() as usize % new_size;
                new_buckets[index].push((key, value));
            }
        }

        self.buckets = new_buckets;
    }

    #[inline(always)]
    pub fn key_set(&self) -> impl Iterator<Item = &(u64, T)> {
        self.buckets.iter().flat_map(|bucket| bucket.iter())
    }
}

impl<T> IntoIterator for HashTable<T> {
    type Item = (u64, T);
    type IntoIter = IntoIter<(u64, T)>;

    #[inline(always)]
    fn into_iter(self) -> Self::IntoIter {
        let mut records = Vec::new();

        for bucket in self.buckets {
            for item in bucket {
                records.push(item);
            }
        }

        records.into_iter()
    }
}

pub trait Hash {
    fn hash(&self) -> u64;
}

impl Hash for u64 {
    #[inline(always)]
    fn hash(&self) -> u64 {
        *self
    }
}

impl Hash for &[u8] {
    #[inline(always)]
    fn hash(&self) -> u64 {
        let mut hash = 0u64;

        for &byte in self.iter() {
            hash = hash.rotate_left(3) ^ byte as u64;
        }

        hash
    }
}
