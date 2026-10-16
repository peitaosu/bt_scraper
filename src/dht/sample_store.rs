use crate::InfoHash;
use std::collections::{HashSet, VecDeque};

const DEFAULT_MAX: usize = 2000;
const DEFAULT_INTERVAL_SECS: u64 = 60;
const MAX_SAMPLE_REPLY: usize = 20;

pub struct SampleStore {
    order: VecDeque<InfoHash>,
    index: HashSet<InfoHash>,
    max: usize,
    interval_secs: u64,
}

impl SampleStore {
    pub fn new(max: usize) -> Self {
        Self {
            order: VecDeque::new(),
            index: HashSet::new(),
            max: max.max(1),
            interval_secs: DEFAULT_INTERVAL_SECS,
        }
    }

    pub fn default_cap() -> Self {
        Self::new(DEFAULT_MAX)
    }

    pub fn record(&mut self, info_hash: InfoHash) {
        if !self.index.insert(info_hash) {
            return;
        }
        if self.order.len() >= self.max {
            if let Some(removed) = self.order.pop_front() {
                self.index.remove(&removed);
            }
        }
        self.order.push_back(info_hash);
    }

    pub fn count(&self) -> usize {
        self.order.len()
    }

    pub fn interval_secs(&self) -> u64 {
        self.interval_secs
    }

    pub fn random_sample(&self, limit: usize) -> Vec<InfoHash> {
        let limit = limit.min(MAX_SAMPLE_REPLY).min(self.order.len());
        if limit == 0 {
            return Vec::new();
        }
        let mut indices: Vec<usize> = (0..self.order.len()).collect();
        for slot in 0..limit {
            let swap_with = rand::random::<usize>() % self.order.len();
            indices.swap(slot, swap_with);
        }
        indices
            .into_iter()
            .take(limit)
            .filter_map(|index| self.order.get(index).copied())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_and_samples_unique_hashes() {
        let mut store = SampleStore::new(4);
        for i in 0u8..6 {
            store.record(InfoHash([i; 20]));
        }
        assert_eq!(store.count(), 4);
        let sample = store.random_sample(2);
        assert_eq!(sample.len(), 2);
    }
}
