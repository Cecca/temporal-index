use crate::nested_vecs::NestedVecs;
use crate::types::*;
use deepsize::DeepSizeOf;
use std::iter::FromIterator;

#[derive(DeepSizeOf)]
pub struct PeriodIndexPlusPlus {
    /// the number of buckets in which each dimension is divided
    num_buckets: usize,
    index: Option<RangeIndex<RangeIndex<RangeIndex<NestedVecs>>>>,
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
        7
    }

    fn index(&mut self, dataset: &[Interval]) {
        self.clear();
        let n_buckets = self.num_buckets;

        let index = RangeIndex::new(
            n_buckets,
            dataset,
            |interval| interval.duration(),
            |by_duration| {
                RangeIndex::new(
                    n_buckets,
                    by_duration,
                    |interval| interval.start,
                    |by_start| {
                        // Vec::from_iter(by_start.iter().map(|interval| ***interval)),
                        RangeIndex::new(
                            n_buckets,
                            by_start,
                            |interval| interval.end,
                            |by_end| {
                                //
                                let local =
                                    Vec::from_iter(by_end.iter().map(|interval| ****interval));
                                let mut inner = NestedVecs::default();
                                inner.index(&local);
                                inner
                            },
                            // |by_end| Vec::from_iter(by_end.iter().map(|interval| ****interval)),
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
                    .query_between(
                        duration.min,
                        duration.max,
                        |(duration_min_bound, duration_max_bound), by_start| {
                            let bucket_duration_range = DurationRange {
                                min: duration_min_bound,
                                max: duration_max_bound,
                            };
                            by_start.query_le(range.end, |start_bound, by_end| {
                                by_end.query_ge(range.start, |end_bound, intervals| {
                                    let bucket_time_range = Interval {
                                        start: start_bound.0,
                                        end: end_bound.1,
                                    };
                                    match (
                                        duration.contains_duration(bucket_duration_range),
                                        range.contains_interval(&bucket_time_range),
                                    ) {
                                        (true, true) => {
                                            // no verification is necessary, the bucket is contained in all dimensions in the query
                                            intervals.for_each(|count, interval| {
                                                debug_assert!(range.overlaps(&interval));
                                                debug_assert!(duration.contains(&interval));
                                                cnt += count as u32;
                                                for _ in 0..count {
                                                    answer.push(interval);
                                                }
                                            });
                                        }
                                        (true, false) => {
                                            // we can skip the verification of the duration
                                            cnt +=
                                                intervals.query_range(range, |count, interval| {
                                                    debug_assert!(duration.contains(&interval));
                                                    debug_assert!(range.overlaps(&interval));
                                                    for _ in 0..count {
                                                        answer.push(interval);
                                                    }
                                                });
                                        }
                                        (false, true) => {
                                            // we can skip the verification of the overlap
                                            cnt += intervals.query_duration(
                                                duration,
                                                |count, interval| {
                                                    debug_assert!(duration.contains(&interval));
                                                    debug_assert!(range.overlaps(&interval));
                                                    for _ in 0..count {
                                                        answer.push(interval);
                                                    }
                                                },
                                            );
                                        }
                                        (false, false) => {
                                            // We have to verify both duration and time range
                                            cnt += intervals.query_range_duration(
                                                range,
                                                duration,
                                                |count, interval| {
                                                    debug_assert!(duration.contains(&interval));
                                                    debug_assert!(range.overlaps(&interval));
                                                    for _ in 0..count {
                                                        answer.push(interval);
                                                    }
                                                },
                                            );
                                        }
                                    }
                                })
                            })
                        },
                    );
                answer.inc_examined(cnt);
            }
            (Some(range), None) => {
                let mut cnt = 0;
                self.index
                    .as_ref()
                    .expect("index not populated")
                    .for_each(|by_start| {
                        by_start.query_le(range.end, |start_bound, by_end| {
                            by_end.query_ge(range.start, |end_bound, intervals| {
                                let bucket_time_range = Interval {
                                    start: start_bound.0,
                                    end: end_bound.1,
                                };
                                if range.contains_interval(&bucket_time_range) {
                                    intervals.for_each(|count, interval| {
                                        debug_assert!(range.overlaps(&interval));
                                        cnt += count as u32;
                                        for _ in 0..count {
                                            answer.push(interval);
                                        }
                                    });
                                } else {
                                    cnt += intervals.query_range(range, |count, interval| {
                                        debug_assert!(range.overlaps(&interval));
                                        for _ in 0..count {
                                            answer.push(interval);
                                        }
                                    });
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
                                by_start.for_each(|by_end| {
                                    by_end.for_each(|intervals| {
                                        intervals.for_each(|count, interval| {
                                            debug_assert!(duration.contains(&interval));
                                            cnt += count as u32;
                                            for _ in 0..count {
                                                answer.push(interval);
                                            }
                                        });
                                    })
                                })
                            } else {
                                by_start.for_each(|by_end| {
                                    by_end.for_each(|intervals| {
                                        cnt += intervals.query_duration(
                                            duration,
                                            |count, interval| {
                                                debug_assert!(duration.contains(&interval));
                                                for _ in 0..count {
                                                    answer.push(interval);
                                                }
                                            },
                                        );
                                    })
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

// Due to Rust's trait object's limitations we cannot use dynamic dispatch, so I
// am using this enum as an approximation
#[derive(DeepSizeOf)]
enum RangeIndex<V> {
    Plain(SortedIndex<V>),
    Block(SortedBlockIndex<V>),
}

impl<V> RangeIndex<V> {
    fn new<D: std::fmt::Debug, I, F, B>(n_buckets: usize, items: I, key: F, builder: B) -> Self
    where
        I: IntoIterator<Item = D>,
        F: Fn(&D) -> Time,
        B: Fn(&[D]) -> V,
    {
        let items = Vec::from_iter(items);
        if items.len() > n_buckets {
            Self::Block(SortedBlockIndex::new(n_buckets, items, key, builder))
        } else {
            Self::Plain(SortedIndex::new(items, key, builder))
        }
    }

    fn for_each<F: FnMut(&V)>(&self, action: F) {
        match self {
            Self::Plain(inner) => inner.for_each(action),
            Self::Block(inner) => inner.for_each(action),
        }
    }
    fn query_between<F: FnMut((Time, Time), &V)>(&self, min: Time, max: Time, action: F) {
        match self {
            Self::Plain(inner) => inner.query_between(min, max, action),
            Self::Block(inner) => inner.query_between(min, max, action),
        }
    }

    #[allow(dead_code)]
    fn query_ge<F: FnMut((Time, Time), &V)>(&self, x: Time, action: F) {
        match self {
            Self::Plain(inner) => inner.query_ge(x, action),
            Self::Block(inner) => inner.query_ge(x, action),
        }
    }
    fn query_le<F: FnMut((Time, Time), &V)>(&self, x: Time, action: F) {
        match self {
            Self::Plain(inner) => inner.query_le(x, action),
            Self::Block(inner) => inner.query_le(x, action),
        }
    }
}

#[derive(DeepSizeOf)]
struct SortedBlockIndex<V> {
    boundaries: Vec<Time>,
    values: Vec<V>,
}

impl<V> SortedBlockIndex<V> {
    /// accepts an iterator over the items, a function to extract the key for each
    /// item, and a function to build a value from all the items associated with a
    /// given item
    fn new<D: std::fmt::Debug, I, F, B>(n_buckets: usize, items: I, key: F, builder: B) -> Self
    where
        I: IntoIterator<Item = D>,
        F: Fn(&D) -> Time,
        B: Fn(&[D]) -> V,
    {
        let mut items = Vec::from_iter(items);
        let distribution = ecdf(items.iter().map(|d| key(d)));

        let mut boundaries = Vec::new();

        let step = (items.len() as u32 / n_buckets as u32) + 1;
        let mut count_threshold = step;
        for (k, &count) in distribution.iter().enumerate() {
            let time = k as u32;
            if count > count_threshold {
                boundaries.push(time);
                count_threshold = count + step;
            }
        }
        // push the last boundary
        if boundaries.last().unwrap() != &(distribution.len() as u32 - 1) {
            boundaries.push(distribution.len() as u32 - 1);
        }

        // go over all the items, sorted by key, define the ranges and build the
        // inner values
        let mut values = Vec::new();
        items.sort_unstable_by_key(|x| key(x));
        let mut start_index = 0;
        let mut end_index = 0;
        loop {
            let current_group = Self::index_for(key(&items[start_index]), &boundaries);
            while end_index < items.len()
                && Self::index_for(key(&items[end_index]), &boundaries) == current_group
            {
                end_index += 1;
            }
            values.push(builder(&items[start_index..end_index]));
            if end_index >= items.len() {
                break;
            }
            start_index = end_index;
        }

        assert_eq!(
            values.len(),
            boundaries.len(),
            "num_buckets: {}, n: {}",
            n_buckets,
            items.len()
        );

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
            while i < boundaries.len() && boundaries[i] < key {
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

    fn for_each<F: FnMut(&V)>(&self, action: F) {
        self.values.iter().for_each(action);
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
        let start = Self::index_for(x, &self.boundaries);
        for i in start..self.boundaries.len() {
            let lower_bound = if i > 0 { self.boundaries[i - 1] } else { 0 };
            let upper_bound = self.boundaries[i];
            action((lower_bound, upper_bound), &self.values[i]);
        }
    }

    fn query_le<F: FnMut((Time, Time), &V)>(&self, x: Time, mut action: F) {
        let end = std::cmp::min(Self::index_for(x, &self.boundaries), self.values.len() - 1);
        debug_assert!(end < self.values.len());
        for i in 0..=end {
            let lower_bound = if i > 0 { self.boundaries[i - 1] } else { 0 };
            let upper_bound = self.boundaries[i];
            action((lower_bound, upper_bound), &self.values[i]);
        }
    }
}

#[derive(DeepSizeOf)]
struct SortedIndex<V> {
    keys: Vec<Time>,
    values: Vec<V>,
}

#[allow(dead_code)]
impl<V> SortedIndex<V> {
    /// accepts an iterator over the items, a function to extract the key for each
    /// item, and a function to build a value from all the items associated with a
    /// given key
    fn new<D: std::fmt::Debug, I, F, B>(items: I, key: F, builder: B) -> Self
    where
        I: IntoIterator<Item = D>,
        F: Fn(&D) -> Time,
        B: Fn(&[D]) -> V,
    {
        let mut items = Vec::from_iter(items);
        items.sort_unstable_by_key(|x| key(x));
        let mut keys = Vec::from_iter(items.iter().map(|x| key(x)));
        keys.dedup();

        let mut values = Vec::new();
        let mut start_index = 0;
        let mut end_index = 0;
        loop {
            let current_key = key(&items[start_index]);
            while end_index < items.len() && key(&items[end_index]) == current_key {
                end_index += 1;
            }
            values.push(builder(&items[start_index..end_index]));
            if end_index >= items.len() {
                break;
            }
            start_index = end_index;
        }

        assert_eq!(keys.len(), values.len());

        Self { keys, values }
    }

    // get the first position >= to the search key
    fn index_for(&self, key: Time) -> usize {
        // a linear search should be OK with few keys
        if self.keys.len() < 128 {
            let mut i = 0;
            while i < self.keys.len() && self.keys[i] < key {
                i += 1;
            }
            i
        } else {
            match self.keys.binary_search(&key) {
                Ok(i) => i,
                Err(i) => i,
            }
        }
    }

    fn for_each<F: FnMut(&V)>(&self, action: F) {
        self.values.iter().for_each(action);
    }

    fn query_between<F: FnMut((Time, Time), &V)>(&self, min: Time, max: Time, mut action: F) {
        let start = self.index_for(min);
        let end = std::cmp::min(self.index_for(max), self.values.len() - 1);
        debug_assert!(end < self.values.len());
        let keys = self.keys[start..=end].iter().map(|t| (*t, *t + 1));
        keys.zip(self.values[start..=end].iter())
            .for_each(|(bounds, values)| action(bounds, values));
    }

    fn query_ge<F: FnMut((Time, Time), &V)>(&self, x: Time, mut action: F) {
        let start = self.index_for(x);
        for i in start..self.keys.len() {
            let lower_bound = if i > 0 { self.keys[i - 1] } else { 0 };
            let upper_bound = self.keys[i];
            action((lower_bound, upper_bound), &self.values[i]);
        }
    }

    fn query_le<F: FnMut((Time, Time), &V)>(&self, x: Time, mut action: F) {
        let end = std::cmp::min(self.index_for(x), self.values.len() - 1);
        debug_assert!(end < self.values.len());
        for i in 0..=end {
            let lower_bound = if i > 0 { self.keys[i - 1] } else { 0 };
            let upper_bound = self.keys[i];
            action((lower_bound, upper_bound), &self.values[i]);
        }
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
        let index = SortedBlockIndex::new(2, intervals, |x| x.start, |xs| Vec::from(xs));
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
        let index = SortedBlockIndex::new(2, intervals, |x| x.end, |xs| Vec::from(xs));
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
        let index = SortedBlockIndex::new(2, intervals, |x| x.duration(), |xs| Vec::from(xs));
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
