use crate::types::*;
use deepsize::DeepSizeOf;
use std::iter::FromIterator;

#[derive(DeepSizeOf)]
pub struct PeriodIndexPlusPlus {
    // num_buckets: usize,
    page_size: usize,
    index: Option<RangeIndex<RangeIndex<Vec<Interval>>>>,
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
        12
    }

    fn index(&mut self, dataset: &[Interval]) {
        self.clear();
        let n = dataset.len();
        let n_buckets_duration =
            ((n + 1) as f64 / (self.page_size * self.page_size) as f64).ceil() as usize;

        let mut pl = progress_logger::ProgressLogger::builder()
            .with_expected_updates(dataset.len() as u64)
            .with_items_name("intervals")
            .start();

        let index = RangeIndex::new(
            n_buckets_duration,
            dataset,
            |interval| interval.duration(),
            |by_duration| {
                let n_buckets_start =
                    ((by_duration.len() + 1) as f64 / self.page_size as f64).ceil() as usize;
                RangeIndex::new(
                    n_buckets_start,
                    by_duration,
                    |interval| interval.start,
                    |by_start| {
                        let mut intervals =
                            Vec::from_iter(by_start.iter().map(|interval| ***interval));
                        intervals.sort_unstable_by_key(|x| x.end);
                        trace!(" .. {} intervals", intervals.len());
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

/// return a vector such that, at position i, we store the count of elements
/// strictly less than i.
fn ecdf_by<T, K: Fn(&T) -> Time>(values: &[T], key_fn: K) -> Vec<u32> {
    let max_key = values.iter().map(&key_fn).max().unwrap();
    let mut ecdf = vec![0u32; max_key as usize + 1];
    let mut n = 0;
    for v in values {
        let k = key_fn(v);
        ecdf[k as usize] += 1u32;
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
        B: FnMut(&[D]) -> V,
    {
        let items = Vec::from_iter(items);
        if items.len() > n_buckets {
            Self::Block(SortedBlockIndex::new(n_buckets, items, key, builder))
        } else {
            Self::Plain(SortedIndex::new(items, key, builder))
        }
    }

    fn for_each<F: FnMut((Time, Time), &V)>(&self, action: F) {
        match self {
            Self::Plain(inner) => todo!(), // inner.for_each(action),
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
    fn new<D: std::fmt::Debug, I, F, B>(n_buckets: usize, items: I, key: F, mut builder: B) -> Self
    where
        I: IntoIterator<Item = D>,
        F: Fn(&D) -> Time,
        B: FnMut(&[D]) -> V,
    {
        let mut items = Vec::from_iter(items);
        let distribution = ecdf_by(&items, &key);

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
        if boundaries.is_empty() || (boundaries.last().unwrap() != &(distribution.len() as u32 - 1))
        {
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
    fn new<D: std::fmt::Debug, I, F, B>(items: I, key: F, mut builder: B) -> Self
    where
        I: IntoIterator<Item = D>,
        F: Fn(&D) -> Time,
        B: FnMut(&[D]) -> V,
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
        for i in (0..self.keys.len()).rev() {
            let lower_bound = if i > 0 { self.keys[i - 1] } else { 0 };
            let upper_bound = self.keys[i];
            if upper_bound < x {
                break;
            }
            action((lower_bound, upper_bound), &self.values[i]);
        }
    }

    fn query_le<F: FnMut((Time, Time), &V)>(&self, x: Time, mut action: F) {
        for i in 0..self.keys.len() {
            let lower_bound = if i > 0 { self.keys[i - 1] } else { 0 };
            let upper_bound = self.keys[i];
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
