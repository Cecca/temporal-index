use crate::types::*;
use deepsize::DeepSizeOf;
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
    fn parameters(&self) -> String {
        String::new()
    }
    fn version(&self) -> u8 {
        2
    }
    fn index(&mut self, dataset: &[Interval]) {
        self.data.clear();
        for interval in dataset {
            self.data
                .entry(interval.duration())
                .or_insert_with(|| Vec::new())
                .push(*interval);
        }
        let size = self.deep_size_of();
        info!(
            "Allocated for index: {} bytes ({} Mb)",
            size,
            size / (1024 * 1024)
        );
    }

    fn query(&self, query: &Query, answers: &mut QueryAnswerBuilder) {
        match (query.range, query.duration) {
            (Some(range), Some(duration)) => {
                for (_duration, intervals) in self.data.range(duration.min..=duration.max) {
                    for interval in intervals {
                        debug_assert!(duration.contains(interval));
                        if range.overlaps(interval) {
                            answers.push(*interval);
                        }
                    }
                }
            }
            (Some(range), None) => {
                for (_duration, intervals) in self.data.iter() {
                    for interval in intervals {
                        if range.overlaps(interval) {
                            answers.push(*interval);
                        }
                    }
                }
            }
            (None, Some(duration)) => {
                for (_duration, intervals) in self.data.range(duration.min..=duration.max) {
                    for interval in intervals {
                        debug_assert!(duration.contains(interval));
                        answers.push(*interval);
                    }
                }
            }
            (None, None) => {
                for (_duration, intervals) in self.data.iter() {
                    for interval in intervals {
                        answers.push(*interval);
                    }
                }
            }
        }
    }

    fn clear(&mut self) {
        self.data.clear();
    }
}

impl deepsize::DeepSizeOf for BTree {
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        // I actually don't know the overhead of the BTReeMap
        self.data.iter().fold(0, |sum, (key, val)| {
            sum + key.deep_size_of_children(context) + val.deep_size_of_children(context)
        })
    }
}
