use std::mem;

use crate::types::*;
use rstar::{RTree, RTreeObject, AABB};

impl RTreeObject for Interval {
    type Envelope = AABB<[i64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_point([self.start as i64, self.duration() as i64])
    }
}

#[derive(Debug, Default)]
pub struct RTreeIndex {
    tree: RTree<Interval>,
    max_start: Time,
    max_duration: Time,
}

impl deepsize::DeepSizeOf for RTreeIndex {
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        0
    }
}

impl Algorithm for RTreeIndex {
    fn name(&self) -> String {
        "RTree".to_owned()
    }

    fn parameters(&self) -> String {
        String::new()
    }

    fn version(&self) -> u8 {
        1
    }

    fn index(&mut self, dataset: &[Interval]) {
        self.max_duration = dataset.iter().map(|i| i.duration()).max().unwrap();
        self.max_start = dataset.iter().map(|i| i.start).max().unwrap();
        let items = Vec::from(dataset);
        let mut tree = RTree::bulk_load(items);
        mem::swap(&mut self.tree, &mut tree);
        drop(tree);
    }

    fn query(&self, query: &Query, answer: &mut QueryAnswerBuilder) {
        let mut examined = 0;
        match (query.range, query.duration) {
            (Some(range), Some(duration)) => {
                let lower_start = if range.start > duration.max {
                    range.start - duration.max
                } else {
                    0
                };
                let lower = [lower_start as i64, duration.min as i64];
                let upper = [range.end as i64, duration.max as i64];
                let envelope = AABB::from_corners(lower, upper);
                for interval in self.tree.locate_in_envelope(&envelope) {
                    examined += 1;
                    if duration.contains(interval) && interval.overlaps(&range) {
                        answer.push(*interval);
                    }
                }
            }
            (Some(range), None) => {
                // This is not ideal, and is a difference with the RD-index. In the RD-index we can
                // use the maximum duration of a column, here we have to use the maximum
                // duration in the entire dataset
                let lower_start = if range.start > self.max_duration {
                    range.start - self.max_duration
                } else {
                    0
                };
                let lower = [lower_start as i64, 0 as i64];
                let upper = [range.end as i64, std::i64::MAX];
                let envelope = AABB::from_corners(lower, upper);
                if range.start == 14 && range.end == 6563 {
                    println!("envelope: {:?}", envelope);
                }
                for interval in self.tree.locate_in_envelope(&envelope) {
                    examined += 1;
                    if interval.overlaps(&range) {
                        answer.push(*interval);
                    }
                }
            }
            (None, Some(duration)) => {
                let lower = [0 as i64, duration.min as i64];
                let upper = [std::i64::MAX, duration.max as i64];
                let envelope = AABB::from_corners(lower, upper);
                for interval in self.tree.locate_in_envelope(&envelope) {
                    examined += 1;
                    if duration.contains(interval) {
                        answer.push(*interval);
                    }
                }
            }
            (None, None) => unimplemented!("Enumeration not supported"),
        }
        answer.inc_examined(examined);
    }

    fn clear(&mut self) {
        self.max_start = 0;
        self.max_duration = 0;
        let mut empty = RTree::new();
        mem::swap(&mut self.tree, &mut empty);
        drop(empty);
    }
}
