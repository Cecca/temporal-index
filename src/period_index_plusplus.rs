use crate::types::*;
use deepsize::DeepSizeOf;
use std::iter::FromIterator;

#[derive(DeepSizeOf)]
pub struct PeriodIndexPlusPlus {
    page_size: usize,
    index: Option<SortedBlockIndex<SortedBlockIndex<Vec<Interval>>>>,
}

impl PeriodIndexPlusPlus {
    pub fn new(page_size: usize) -> Self {
        Self {
            page_size,
            index: None,
        }
    }
}

impl std::fmt::Debug for PeriodIndexPlusPlus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "period-index++({})", self.page_size)
    }
}

impl Algorithm for PeriodIndexPlusPlus {
    fn name(&self) -> String {
        "period-index++".to_owned()
    }

    fn parameters(&self) -> String {
        format!("page_size={}", self.page_size)
    }

    fn version(&self) -> u8 {
        15
    }

    fn index(&mut self, dataset: &[Interval]) {
        self.clear();

        let mut dataset: Vec<Interval> = Vec::from_iter(dataset.iter().copied());

        let mut pl = progress_logger::ProgressLogger::builder()
            .with_expected_updates(dataset.len() as u64)
            .with_items_name("intervals")
            .start();

        let index = SortedBlockIndex::new(
            self.page_size * self.page_size,
            &mut dataset,
            |interval| interval.duration(),
            |by_duration| {
                SortedBlockIndex::new(
                    self.page_size,
                    by_duration,
                    |interval| interval.start,
                    |by_start| {
                        let mut intervals =
                            Vec::from_iter(by_start.iter().map(|interval| *interval));
                        intervals.sort_unstable_by_key(|x| x.end);
                        pl.update(intervals.len() as u64);
                        intervals
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
                    .query_between(
                        duration.min,
                        duration.max,
                        |(duration_min_bound, duration_max_bound), by_start| {
                            let bucket_duration_range = DurationRange {
                                min: duration_min_bound,
                                max: duration_max_bound,
                            };
                            let start_time = if range.start > duration_max_bound {
                                range.start - duration_max_bound
                            } else {
                                0
                            };
                            by_start.query_between(
                                start_time,
                                range.end,
                                |start_bound, sorted_by_end| {
                                    match (
                                        duration.contains_duration(bucket_duration_range),
                                        start_bound.1 < range.end,
                                    ) {
                                        // don't check overlap or duration, they are guaranteed
                                        (true, true) => sorted_by_end
                                            .iter()
                                            .rev()
                                            .take_while(|interval| interval.end > range.start)
                                            .for_each(|interval| {
                                                cnt += 1;
                                                debug_assert!(duration.contains(interval));
                                                debug_assert!(range.overlaps(interval));
                                                answer.push(*interval);
                                            }),
                                        // check only the overlap
                                        (true, false) => sorted_by_end
                                            .iter()
                                            .rev()
                                            .take_while(|interval| interval.end > range.start)
                                            .for_each(|interval| {
                                                cnt += 1;
                                                if range.overlaps(interval) {
                                                    debug_assert!(duration.contains(interval));
                                                    answer.push(*interval);
                                                }
                                            }),
                                        // check only the duration
                                        (false, true) => sorted_by_end
                                            .iter()
                                            .rev()
                                            .take_while(|interval| interval.end > range.start)
                                            .for_each(|interval| {
                                                cnt += 1;
                                                if duration.contains(interval) {
                                                    debug_assert!(range.overlaps(interval));
                                                    answer.push(*interval);
                                                }
                                            }),
                                        // check both duration and overlap
                                        (false, false) => sorted_by_end
                                            .iter()
                                            .rev()
                                            .take_while(|interval| interval.end > range.start)
                                            .for_each(|interval| {
                                                cnt += 1;
                                                if duration.contains(interval)
                                                    && range.overlaps(interval)
                                                {
                                                    answer.push(*interval);
                                                }
                                            }),
                                    }
                                },
                            );
                        },
                    );
                answer.inc_examined(cnt);
            }
            (Some(range), None) => {
                let mut cnt = 0;
                self.index.as_ref().expect("index not populated").for_each(
                    |(_duration_min, duration_max), by_start| {
                        let start_time = if range.start > duration_max {
                            range.start - duration_max
                        } else {
                            0
                        };
                        by_start.query_between(
                            start_time,
                            range.end,
                            |start_bound, sorted_by_end| {
                                if start_bound.1 < range.end {
                                    // we don't need to check for the range
                                    sorted_by_end
                                        .iter()
                                        .rev()
                                        .take_while(|interval| interval.end > range.start)
                                        .for_each(|interval| {
                                            cnt += 1;
                                            answer.push(*interval);
                                        })
                                } else {
                                    sorted_by_end
                                        .iter()
                                        .rev()
                                        .take_while(|interval| interval.end > range.start)
                                        .for_each(|interval| {
                                            cnt += 1;
                                            if range.overlaps(interval) {
                                                answer.push(*interval);
                                            }
                                        })
                                }
                            },
                        )
                    },
                );
                answer.inc_examined(cnt);
            }
            (None, Some(duration)) => {
                let mut cnt = 0;
                self.index
                    .as_ref()
                    .expect("index not populated")
                    .query_between(
                        duration.min,
                        duration.max,
                        |(duration_min_bound, duration_max_bound), by_start| {
                            let bucket_duration_range = DurationRange {
                                min: duration_min_bound,
                                max: duration_max_bound,
                            };
                            if duration.contains_duration(bucket_duration_range) {
                                // skip verification, just enumerate
                                by_start.for_each(|_, sorted_by_end| {
                                    for interval in sorted_by_end {
                                        cnt += 1;
                                        debug_assert!(duration.contains(interval));
                                        answer.push(*interval);
                                    }
                                })
                            } else {
                                by_start.for_each(|_, sorted_by_end| {
                                    for interval in sorted_by_end {
                                        cnt += 1;
                                        let matches_duration = duration.contains(interval);
                                        if matches_duration {
                                            answer.push(*interval);
                                        }
                                    }
                                })
                            }
                        },
                    );
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

#[derive(DeepSizeOf)]
struct SortedBlockIndex<V> {
    boundaries: Vec<Time>,
    values: Vec<V>,
}

impl<V: Send + Sync> SortedBlockIndex<V> {
    /// accepts an iterator over the items, a function to extract the key for each
    /// item, and a function to build a value from all the items associated with a
    /// given item
    fn new<D: std::fmt::Debug, F, B>(
        bucket_size: usize,
        items: &mut [D],
        key: F,
        mut builder: B,
    ) -> Self
    where
        F: Fn(&D) -> Time,
        B: FnMut(&mut [D]) -> V,
    {
        let mut values = Vec::new();
        let mut boundaries = Vec::new();

        items.sort_unstable_by_key(&key);

        let mut current_index = 0;
        while current_index < items.len() {
            let end_index = if current_index + bucket_size >= items.len() {
                // this is the last bucket
                items.len()
            } else {
                let lookahead = current_index + bucket_size;
                let lookahead_key = key(&items[lookahead]);
                let mut end_index = lookahead;
                if lookahead_key == key(&items[current_index]) {
                    // we are in a heavy bucket, skip to the end
                    while end_index < items.len() && key(&items[end_index]) == lookahead_key {
                        end_index += 1;
                    }
                } else {
                    // we are not in a heavy bucket, go back until the previous key
                    while end_index >= current_index && key(&items[end_index]) == lookahead_key {
                        end_index -= 1;
                    }
                    end_index += 1;
                }
                end_index
            };
            let boundary = if end_index == items.len() {
                std::u32::MAX
            } else {
                key(&items[end_index])
            };
            boundaries.push(boundary);
            values.push(builder(&mut items[current_index..end_index]));
            // jump to the beginning of the next bucket
            current_index = end_index;
        }

        Self {
            boundaries,
            values: values,
        }
    }

    /// returns the group in which the provided key falls in. Recall that
    /// boundaries are upper boundaries (excluded)
    fn index_for(key: Time, boundaries: &[Time]) -> usize {
        if boundaries.len() < 128 {
            let mut i = 0;
            while i < boundaries.len() && boundaries[i] <= key {
                i += 1;
            }
            i
        } else {
            match boundaries.binary_search(&key) {
                Ok(i) => i,
                Err(i) => i,
            }
        }
    }

    fn for_each<F: FnMut((Time, Time), &V)>(&self, mut action: F) {
        // self.values.iter().for_each(action);
        for i in 0..self.boundaries.len() {
            let lower_bound = if i > 0 { self.boundaries[i - 1] } else { 0 };
            let upper_bound = self.boundaries[i];
            action((lower_bound, upper_bound), &self.values[i]);
        }
    }

    fn query_between<F: FnMut((Time, Time), &V)>(&self, min: Time, max: Time, mut action: F) {
        let start = Self::index_for(min, &self.boundaries);
        let end = std::cmp::min(
            Self::index_for(max, &self.boundaries),
            self.values.len() - 1,
        );
        debug_assert!(end < self.values.len());
        // self.values[start..=end].iter().for_each(action);
        for i in start..=end {
            let lower_bound = if i > 0 { self.boundaries[i - 1] } else { 0 };
            let upper_bound = self.boundaries[i];
            action((lower_bound, upper_bound), &self.values[i]);
        }
    }

    #[allow(dead_code)]
    fn query_ge<F: FnMut((Time, Time), &V)>(&self, x: Time, mut action: F) {
        for i in (0..self.boundaries.len()).rev() {
            let lower_bound = if i > 0 { self.boundaries[i - 1] } else { 0 };
            let upper_bound = self.boundaries[i];
            if upper_bound < x {
                break;
            }
            action((lower_bound, upper_bound), &self.values[i]);
        }
    }

    #[allow(dead_code)]
    fn query_le<F: FnMut((Time, Time), &V)>(&self, x: Time, mut action: F) {
        // let end = std::cmp::min(Self::index_for(x, &self.boundaries), self.values.len() - 1);
        // debug_assert!(end < self.values.len());
        for i in 0..self.values.len() {
            let lower_bound = if i > 0 { self.boundaries[i - 1] } else { 0 };
            let upper_bound = self.boundaries[i];
            if lower_bound > x {
                break;
            }
            action((lower_bound, upper_bound), &self.values[i]);
        }
    }
}
#[cfg(test)]
mod test {
    use super::*;

    // #[test]
    fn test_build_start() {
        let mut intervals = vec![
            Interval { start: 8, end: 10 },
            Interval { start: 1, end: 13 },
            Interval { start: 2, end: 9 },
            Interval { start: 12, end: 19 },
            Interval { start: 12, end: 22 },
            Interval { start: 4, end: 8 },
        ];
        let index = SortedBlockIndex::new(2, &mut intervals, |x| x.start, |xs| Vec::from(xs));
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
        let mut intervals = vec![
            Interval { start: 8, end: 10 },
            Interval { start: 1, end: 13 },
            Interval { start: 2, end: 9 },
            Interval { start: 12, end: 19 },
            Interval { start: 12, end: 22 },
            Interval { start: 4, end: 8 },
        ];
        let index = SortedBlockIndex::new(2, &mut intervals, |x| x.end, |xs| Vec::from(xs));
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
        let mut intervals = vec![
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
        let index = SortedBlockIndex::new(2, &mut intervals, |x| x.duration(), |xs| Vec::from(xs));
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
