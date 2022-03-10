use crate::types::*;
use std::collections::BTreeMap;

pub struct BTree {
    data: BTreeMap<Time, Vec<Interval>>,
}

impl BTree {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }
}

impl std::fmt::Debug for BTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BTree")
    }
}

impl Algorithm for BTree {
    fn name(&self) -> String {
        String::from("BTree")
    }
    fn size(&self) -> usize {
        self.data.values().map(|bucket| bucket.len()).sum()
    }
    fn parameters(&self) -> String {
        String::new()
    }
    fn version(&self) -> u8 {
        5
    }
    fn index(&mut self, dataset: &[Interval]) {
        self.data.clear();
        let mut pl = progress_logger::ProgressLogger::builder()
            .with_items_name("intervals")
            .with_expected_updates(dataset.len() as u64)
            .start();
        for interval in dataset {
            self.data
                .entry(interval.duration())
                .or_insert_with(|| Vec::new())
                .push(*interval);
            pl.update_light(1u64);
        }
        pl.stop();
    }

    fn query(&self, query: &Query, answers: &mut QueryAnswerBuilder) {
        match (query.range, query.duration) {
            (Some(range), Some(duration)) => {
                let mut cnt = 0;
                for (_duration, intervals) in self.data.range(duration.min..=duration.max) {
                    for interval in intervals {
                        cnt += 1;
                        debug_assert!(duration.contains(interval));
                        if range.overlaps(interval) {
                            answers.push(*interval);
                        }
                    }
                }
                answers.inc_examined(cnt);
            }
            (Some(range), None) => {
                let mut cnt = 0;
                for (_duration, intervals) in self.data.iter() {
                    for interval in intervals {
                        cnt += 1;
                        if range.overlaps(interval) {
                            answers.push(*interval);
                        }
                    }
                }
                answers.inc_examined(cnt);
            }
            (None, Some(duration)) => {
                let mut cnt = 0;
                for (_duration, intervals) in self.data.range(duration.min..=duration.max) {
                    for interval in intervals {
                        cnt += 1;
                        debug_assert!(duration.contains(interval));
                        answers.push(*interval);
                    }
                }
                answers.inc_examined(cnt);
            }
            (None, None) => {
                let mut cnt = 0;
                for (_duration, intervals) in self.data.iter() {
                    for interval in intervals {
                        cnt += 1;
                        answers.push(*interval);
                    }
                }
                answers.inc_examined(cnt);
            }
        }
    }

    fn clear(&mut self) {
        self.data.clear();
    }

    fn insert(&mut self, interval: Interval) {
        self.data
            .entry(interval.duration())
            .or_insert_with(Vec::new)
            .push(interval);
    }

    fn remove(&mut self, interval: Interval) {
        let bucket = self.data.entry(interval.duration()).or_default();
        bucket.retain(|i| *i != interval);
        if bucket.len() == 0 {
            self.data.remove(&interval.duration());
        }
    }
}
