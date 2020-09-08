use crate::types::*;
use deepsize::DeepSizeOf;
use std::iter::FromIterator;

#[derive(DeepSizeOf)]
pub struct PeriodIndexPlusPlus {
    /// the number of buckets in which each dimension is divided
    num_buckets: usize,
    index: Option<SortedRangeIndex<SortedRangeIndex<SortedRangeIndex<Vec<Interval>>>>>,
}

impl PeriodIndexPlusPlus {
    pub fn new(num_buckets: usize) -> Self {
        Self {
            num_buckets,
            index: None,
        }
    }
}

impl std::fmt::Debug for PeriodIndexPlusPlus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "period-index++({})", self.num_buckets)
    }
}

impl Algorithm for PeriodIndexPlusPlus {
    fn name(&self) -> String {
        "period-index++".to_owned()
    }

    fn parameters(&self) -> String {
        format!("{}", self.num_buckets)
    }

    fn version(&self) -> u8 {
        1
    }

    fn index(&mut self, dataset: &[Interval]) {
        self.clear();
        let n_buckets = self.num_buckets;

        let index = SortedRangeIndex::new(
            n_buckets,
            dataset,
            |interval| interval.duration(),
            |by_duration| {
                info!("index by start time {:?} elements", by_duration.len());
                SortedRangeIndex::new(
                    n_buckets,
                    by_duration,
                    |interval| interval.start,
                    |by_start| {
                        info!("index by end time {:?} elements", by_start.len());
                        SortedRangeIndex::new(
                            n_buckets,
                            by_start,
                            |interval| interval.end,
                            |by_end| Vec::from_iter(by_end.iter().map(|interval| ****interval)),
                        )
                    },
                )
            },
        );

        self.index.replace(index);

        let size = self.deep_size_of();
        info!(
            "Allocated for index: {} bytes ({} Mb)",
            size,
            size / (1024 * 1024)
        );
    }

    fn query(&self, query: &Query, answer: &mut QueryAnswerBuilder) {
        match (query.range, query.duration) {
            (Some(range), Some(duration)) => {
                let mut cnt = 0;
                self.index
                    .as_ref()
                    .expect("index not populated")
                    .query_between(duration.min, duration.max, |by_start| {
                        by_start.query_le(range.end, |by_end| {
                            by_end.query_ge(range.start, |intervals| {
                                for interval in intervals {
                                    cnt += 1;
                                    let matches_duration = duration.contains(interval);
                                    let overlaps = range.overlaps(interval);
                                    if matches_duration && overlaps {
                                        answer.push(*interval);
                                    }
                                }
                            })
                        })
                    });
                answer.inc_examined(cnt);
            }
            (Some(range), None) => {
                let mut cnt = 0;
                self.index
                    .as_ref()
                    .expect("index not populated")
                    .for_each(|by_start| {
                        by_start.query_le(range.end, |by_end| {
                            by_end.query_ge(range.start, |intervals| {
                                for interval in intervals {
                                    cnt += 1;
                                    let overlaps = range.overlaps(interval);
                                    if overlaps {
                                        answer.push(*interval);
                                    }
                                }
                            })
                        })
                    });
                answer.inc_examined(cnt);
            }
            (None, Some(duration)) => {
                let mut cnt = 0;
                self.index
                    .as_ref()
                    .expect("index not populated")
                    .query_between(duration.min, duration.max, |by_start| {
                        by_start.for_each(|by_end| {
                            by_end.for_each(|intervals| {
                                for interval in intervals {
                                    cnt += 1;
                                    let matches_duration = duration.contains(interval);
                                    if matches_duration {
                                        answer.push(*interval);
                                    }
                                }
                            })
                        })
                    });
                answer.inc_examined(cnt);
            }
            (None, None) => {
                unimplemented!("iteration not supported");
            }
        }
    }

    fn clear(&mut self) {
        self.index.take();
    }
}

/// return a vector such that, at position i, we store the count of elements
/// strictly less than i.
fn ecdf<I: IntoIterator<Item = Time>>(values: I) -> Vec<u32> {
    let mut ecdf = Vec::new();
    let mut n = 0;
    for v in values {
        if v as usize >= ecdf.len() {
            ecdf.resize(v as usize + 1, 0);
        }
        ecdf[v as usize] += 1u32;
        n += 1;
    }
    let mut cumulative_count = 0u32;
    for count in ecdf.iter_mut() {
        cumulative_count += *count;
        *count = cumulative_count;
    }
    assert!(
        n == cumulative_count,
        "n is {}, while the cumulative count is {}",
        n,
        cumulative_count
    );

    ecdf
}

#[derive(DeepSizeOf)]
struct SortedRangeIndex<V> {
    boundaries: Vec<Time>,
    values: Vec<V>,
}

impl<V> SortedRangeIndex<V> {
    /// accepts an iterator over the items, a function to extract the key for each
    /// item, and a function to build a value from all the items associated with a
    /// given item
    #[allow(dead_code)]
    fn new<D: std::fmt::Debug, I, F, B>(n_buckets: usize, items: I, key: F, builder: B) -> Self
    where
        I: IntoIterator<Item = D>,
        F: Fn(&D) -> Time,
        B: Fn(&[D]) -> V,
    {
        let mut items = Vec::from_iter(items);
        let distribution = ecdf(items.iter().map(|d| key(d)));

        let mut boundaries = Vec::new();

        let step = items.len() as u32 / n_buckets as u32;
        let mut count_threshold = step;
        for (k, &count) in distribution.iter().enumerate() {
            let time = k as u32;
            if count > count_threshold {
                boundaries.push(time);
                count_threshold += step;
            }
        }
        // push the last boundary
        if boundaries.last().unwrap() != &(distribution.len() as u32 - 1) {
            boundaries.push(distribution.len() as u32 - 1);
        }
        println!(
            "distribution over {} distinct values, boundaries {:?}",
            distribution.len(),
            boundaries
        );

        // go over all the items, sorted by key, define the ranges and build the
        // inner values
        let mut values = Vec::new();
        items.sort_unstable_by_key(|x| key(x));
        let mut start_index = 0;
        let mut end_index = 0;
        loop {
            let current_group = Self::index_for(key(&items[start_index]), &boundaries);
            println!("current group is {}", current_group);
            while end_index < items.len()
                && Self::index_for(key(&items[end_index]), &boundaries) == current_group
            {
                end_index += 1;
            }
            values.push(builder(&items[dbg!(start_index..end_index)]));
            if end_index >= items.len() {
                break;
            }
            start_index = end_index;
        }

        assert_eq!(values.len(), boundaries.len());

        Self {
            boundaries,
            values: values,
        }
    }

    /// returns the group in which the provided key falls in. Recall that
    /// boundaries are upper boundaries (excluded)
    fn index_for(key: Time, boundaries: &[Time]) -> usize {
        // TODO we might want to introduce binary search for large bucket counts
        // boundaries.iter().take_while(|x| key < **x).count()
        let mut i = 0;
        while i < boundaries.len() && boundaries[i] < key {
            i += 1;
        }
        i
    }

    fn for_each<F: FnMut(&V)>(&self, action: F) {
        self.values.iter().for_each(action);
    }

    fn query_between<F: FnMut(&V)>(&self, min: Time, max: Time, action: F) {
        let start = Self::index_for(min, &self.boundaries);
        let end = std::cmp::min(
            Self::index_for(max, &self.boundaries),
            self.values.len() - 1,
        );
        debug_assert!(end < self.values.len());
        self.values[start..=end].iter().for_each(action);
    }

    fn query_ge<F: FnMut(&V)>(&self, x: Time, action: F) {
        let start = Self::index_for(x, &self.boundaries);
        self.values[start..].iter().for_each(action);
    }

    fn query_le<F: FnMut(&V)>(&self, x: Time, action: F) {
        let end = std::cmp::min(Self::index_for(x, &self.boundaries), self.values.len() - 1);
        debug_assert!(end < self.values.len());
        self.values[..=end].iter().for_each(action);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    // #[test]
    fn test_build_start() {
        let intervals = vec![
            Interval { start: 8, end: 10 },
            Interval { start: 1, end: 13 },
            Interval { start: 2, end: 9 },
            Interval { start: 12, end: 19 },
            Interval { start: 12, end: 22 },
            Interval { start: 4, end: 8 },
        ];
        let index = SortedRangeIndex::new(2, intervals, |x| x.start, |xs| Vec::from(xs));
        assert_eq!(
            index.values,
            vec![
                vec![
                    Interval { start: 1, end: 13 },
                    Interval { start: 2, end: 9 },
                    Interval { start: 4, end: 8 },
                ],
                vec![
                    Interval { start: 8, end: 10 },
                    Interval { start: 12, end: 19 },
                    Interval { start: 12, end: 22 },
                ],
            ]
        )
    }

    // #[test]
    fn test_build_end() {
        let intervals = vec![
            Interval { start: 8, end: 10 },
            Interval { start: 1, end: 13 },
            Interval { start: 2, end: 9 },
            Interval { start: 12, end: 19 },
            Interval { start: 12, end: 22 },
            Interval { start: 4, end: 8 },
        ];
        let index = SortedRangeIndex::new(2, intervals, |x| x.end, |xs| Vec::from(xs));
        assert_eq!(
            index.values,
            vec![
                vec![
                    Interval { start: 4, end: 8 },
                    Interval { start: 2, end: 9 },
                    Interval { start: 8, end: 10 },
                ],
                vec![
                    Interval { start: 1, end: 13 },
                    Interval { start: 12, end: 19 },
                    Interval { start: 12, end: 22 },
                ],
            ]
        )
    }

    // #[test]
    fn test_build_duration() {
        let intervals = vec![
            Interval { start: 8, end: 10 },
            Interval { start: 1, end: 13 },
            Interval { start: 2, end: 9 },
            Interval { start: 1, end: 3 },
            Interval { start: 22, end: 25 },
            Interval { start: 12, end: 19 },
            Interval { start: 12, end: 22 },
            Interval { start: 4, end: 8 },
        ];
        println!(
            "{:?}",
            Vec::from_iter(intervals.iter().map(|x| x.duration()))
        );
        let index = SortedRangeIndex::new(2, intervals, |x| x.duration(), |xs| Vec::from(xs));
        assert_eq!(
            index.values,
            vec![
                vec![
                    Interval { start: 8, end: 10 },
                    Interval { start: 1, end: 3 },
                    Interval { start: 22, end: 25 },
                    Interval { start: 4, end: 8 },
                ],
                vec![
                    Interval { start: 2, end: 9 },
                    Interval { start: 12, end: 19 },
                    Interval { start: 12, end: 22 },
                    Interval { start: 1, end: 13 },
                ],
            ]
        )
    }
}
