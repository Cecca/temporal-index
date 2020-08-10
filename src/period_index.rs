use crate::types::*;
use anyhow::Result;
use deepsize::DeepSizeOf;

#[derive(DeepSizeOf)]
struct Cell {
    time_range: Interval,
    duration_range: DurationRange,
    intervals: Vec<Interval>,
}

impl Cell {
    fn new(time_range: Interval, duration_range: DurationRange) -> Self {
        Self {
            time_range,
            duration_range,
            intervals: Vec::new(),
        }
    }

    fn insert(&mut self, interval: Interval) {
        debug_assert!(
            self.duration_range.contains(&interval),
            "expected duration range {:?}, but trying to insert interval with duration {}",
            self.duration_range,
            interval.duration()
        );
        debug_assert!(self.time_range.overlaps(&interval));
        if interval.start == 9839 && interval.end == 9872 {
            info!("Inserting missing interval");
        }
        self.intervals.push(interval);
    }

    fn query_range_duration<F: FnMut(&Interval)>(
        &self,
        range: Interval,
        duration: DurationRange,
        action: &mut F,
    ) {
        for interval in &self.intervals {
            if duration.contains(interval) && range.overlaps(interval) {
                // Check that this is the first occurrence, which happens if the cell starts
                // before the interval. If the interval starts before the cell,
                // we have still to check if the query covers the previous cell,
                // otherwise it's the first occurrence for _this_ query
                if (self.time_range.start <= interval.start)
                    || (self.time_range.start <= range.start)
                {
                    action(interval);
                }
            }
        }
    }

    fn query_range_only<F: FnMut(&Interval)>(&self, range: Interval, action: &mut F) {
        for interval in &self.intervals {
            if range.overlaps(interval) {
                // Check that this is the first occurrence, which happens if the cell starts
                // before the interval. If the interval starts before the cell,
                // we have still to check if the query covers the previous cell,
                // otherwise it's the first occurrence for _this_ query
                if (self.time_range.start <= interval.start)
                    || (self.time_range.start <= range.start)
                {
                    action(interval);
                }
            }
        }
    }

    fn query_duration_only<F: FnMut(&Interval)>(&self, duration: &DurationRange, action: &mut F) {
        for interval in &self.intervals {
            if duration.contains(interval) {
                // Check that this is the first occurrence, which happens if the cell starts
                // before the interval. If the interval starts before the cell,
                // we have still to check if the query covers the previous cell,
                // otherwise it's the first occurrence for _this_ query
                if self.time_range.start <= interval.start {
                    action(interval);
                }
            }
        }
    }
}

#[derive(DeepSizeOf)]
struct Bucket {
    time_range: Interval,
    /// Two dimensional arrangement of cells: each level holds cells
    /// for intervals of different duration
    cells: Vec<Vec<Cell>>,
}

impl Bucket {
    fn new(time_range: Interval, num_levels: u32) -> Self {
        let mut cells = Vec::with_capacity(num_levels as usize);
        let mut duration = time_range.duration();
        let mut duration_range = DurationRange::new(duration, std::u32::MAX);
        let mut level_count = 0;
        while duration > 0 && level_count < num_levels {
            debug!("level {}, duration range {:?}", level_count, duration_range);
            let mut level = Vec::new();
            let mut start = time_range.start;
            while start < time_range.end {
                let cell = Cell::new(Interval::new(start, duration), duration_range.clone());
                level.push(cell);
                start += duration;
            }
            cells.push(level);
            duration /= 2;
            duration_range.max = duration_range.min;
            duration_range.min = duration;
            level_count += 1;
        }

        // adjust lower bound on duration for the last level
        let last_idx = cells.len() - 1;
        for cell in cells[last_idx].iter_mut() {
            cell.duration_range.min = 0;
        }

        Self { time_range, cells }
    }

    #[inline]
    fn level_for(&self, duration: Time) -> usize {
        std::cmp::min(
            (self.cells.len() - 1) as isize,
            std::cmp::max(
                (self.time_range.duration() as f64 / duration as f64)
                    .log2()
                    .ceil() as isize, // FIXME: In the paper this is a floor, but it doesn't work :/
                0,
            ),
        ) as usize
    }

    #[inline]
    fn cells_for(&self, level: usize, interval: Interval) -> (usize, usize) {
        let cell_duration = self.cells[level][0].time_range.duration();
        let start = if interval.start < self.time_range.start {
            0
        } else {
            ((interval.start - self.time_range.start) / cell_duration) as usize
        };
        let end = if interval.end > self.time_range.end {
            self.cells[level].len() - 1
        } else {
            ((interval.end - self.time_range.start) / cell_duration) as usize
        };
        (start, std::cmp::min(end, self.cells[level].len() - 1))
    }

    fn insert(&mut self, interval: Interval) {
        let level = self.level_for(interval.duration());
        let (start, end) = self.cells_for(level, interval);

        let mut cnt = 0;
        for cell in self.cells[level][start..=end].iter_mut() {
            if cell.time_range.overlaps(&interval) {
                cell.insert(interval);
                cnt += 1;
            }
        }
        assert!(cnt > 0);
    }

    fn query<F: FnMut(&Interval)>(&self, query: &Query, action: &mut F) {
        let (level_min, level_max) = match query.duration {
            None => (0, self.cells.len() - 1),
            Some(duration_range) => {
                let level_min = self.level_for(duration_range.max);
                let level_max = self.level_for(duration_range.min);
                debug_assert!(level_min <= level_max);
                (level_min, level_max)
            }
        };

        // TODO do the match outside the for loop
        for level in level_min..=level_max {
            let (start, end) = query
                .range
                .map(|r| self.cells_for(level, r))
                .unwrap_or_else(|| (0, self.cells.len() - 1));
            match (query.range, query.duration) {
                (Some(range), Some(duration)) => {
                    for cell in self.cells[level][start..=end].iter() {
                        cell.query_range_duration(range, duration, action);
                    }
                }
                (Some(range), None) => {
                    for cell in self.cells[level][start..=end].iter() {
                        cell.query_range_only(range, action);
                    }
                }
                (None, Some(duration)) => {
                    for cell in self.cells[level][start..=end].iter() {
                        cell.query_duration_only(&duration, action);
                    }
                }
                (None, None) => unimplemented!("enumeration not supportedk"),
            }
        }
    }
}

#[derive(DeepSizeOf)]
pub struct PeriodIndex {
    num_buckets: usize,
    num_levels: u32,
    anchor_point: Time,
    bucket_length: Option<Time>,
    n: usize,
    buckets: Vec<Bucket>,
}

impl std::fmt::Debug for PeriodIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "period-index({}, {})", self.num_buckets, self.num_levels)
    }
}

impl PeriodIndex {
    pub fn new(num_buckets: usize, num_levels: u32) -> Result<Self> {
        Ok(Self {
            num_buckets,
            num_levels,
            anchor_point: 0,
            bucket_length: None,
            n: 0,
            buckets: Vec::new(),
        })
    }

    #[inline]
    fn bucket_for(&self, interval: Interval) -> (usize, usize) {
        debug_assert!(interval.start >= self.anchor_point);
        let start = ((std::cmp::max(0, interval.start - self.anchor_point))
            / self.bucket_length.expect("uninitialized index")) as usize;
        let end = ((std::cmp::max(0, interval.end - self.anchor_point))
            / self.bucket_length.expect("uninitialized index")) as usize;
        (start, end)
    }
}

impl Algorithm for PeriodIndex {
    fn name(&self) -> String {
        String::from("period-index")
    }

    fn parameters(&self) -> String {
        format!("{},{}", self.num_buckets, self.num_levels)
    }

    fn version(&self) -> u8 {
        3
    }

    fn index(&mut self, dataset: &[Interval]) {
        let anchor = dataset
            .iter()
            .map(|i| i.start)
            .min()
            .expect("minimum of an empty collection");
        let endtime = dataset
            .iter()
            .map(|i| i.end)
            .max()
            .expect("maximum of an empty collection");
        let bucket_length = ((endtime - anchor) as f64 / self.num_buckets as f64).ceil() as u32;
        debug!("Anchor {}, endtime {}", anchor, endtime);
        self.bucket_length.replace(bucket_length);

        if self.num_levels > ((bucket_length as f64).log2().floor() as u32) {
            warn!("the parameters `num_levels` is larger than the actual number of levels, not using it.");
            self.num_levels = (bucket_length as f64).log2().floor() as u32;
        }

        self.buckets.clear();
        self.anchor_point = anchor;
        self.n = dataset.len();
        let mut start = anchor;
        while start < endtime {
            let bucket = Bucket::new(Interval::new(start, bucket_length), self.num_levels);
            debug!("bucket with interval {:?}", bucket.time_range);
            self.buckets.push(bucket);
            start += bucket_length;
        }
        debug!("Created {} buckets", self.buckets.len());

        for interval in dataset {
            let (start, end) = self.bucket_for(*interval);
            if interval.start == 9839 && interval.end == 9872 {
                info!("Inserting in buckets from {} to {}", start, end);
            }
            let mut cnt = 0;
            assert!(end < self.buckets.len());
            assert!(start <= end);
            for bucket in self.buckets[start..=end].iter_mut() {
                if interval.overlaps(&bucket.time_range) {
                    bucket.insert(*interval);
                    cnt += 1;
                }
            }
            trace!(
                "Inserted into {} buckets, since it has duration {} and the bucket length is {}",
                cnt,
                interval.duration(),
                bucket_length
            );
        }
        let size = self.deep_size_of();
        info!(
            "Allocated for index: {} bytes ({} Mb) - {} buckets",
            size,
            size / (1024 * 1024),
            self.buckets.len()
        );

        debug!(
            "bucket starts {:?}",
            self.buckets
                .iter()
                .map(|b| b.time_range.start)
                .collect::<Vec<Time>>()
        );
    }

    fn query(&self, query: &Query, answer: &mut QueryAnswerBuilder) {
        let (start, end) = query
            .range
            .map(|r| self.bucket_for(r))
            .unwrap_or_else(|| (0, self.buckets.len() - 1));
        debug!("Loooking at buckets from {} to {}", start, end);
        for bucket in self.buckets[start..=end].iter() {
            bucket.query(query, &mut |interval| {
                answer.push(*interval);
            });
        }
    }

    fn clear(&mut self) {
        self.buckets.clear();
        self.anchor_point = 0;
        self.bucket_length.take();
        self.n = 0;
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::dataset::*;
    use crate::naive::*;

    #[test]
    fn test_same_result() {
        let data = RandomDatasetZipfAndUniform::new(123, 10, 1.0, 1000).get();
        let queries = RandomQueriesZipfAndUniform::new(1734, 10, 1.0, 1000, 0.5).get();

        let mut linear_scan = LinearScan::new();
        linear_scan.index(&data);
        let ls_result = linear_scan.run(&queries);

        let mut period_index = PeriodIndex::new(128, 4).unwrap();
        period_index.index(&data);
        let pi_result = period_index.run(&queries);

        for (ls_ans, pi_ans) in ls_result.into_iter().zip(pi_result.into_iter()) {
            assert_eq!(ls_ans.intervals(), pi_ans.intervals());
        }
    }

    #[test]
    fn test_crash() {
        use std::collections::BTreeSet;
        let data = RandomDataset::new(
            123,
            10000,
            TimeDistribution::Uniform {
                low: 1,
                high: 100000,
            },
            TimeDistribution::Zipf {
                n: 1000000,
                beta: 1.0,
            },
        )
        .get();
        let queries = RandomQueryset::new(
            23512,
            5000,
            Some((
                TimeDistribution::Uniform {
                    low: 1,
                    high: 100000,
                },
                TimeDistribution::Zipf {
                    n: 1000000,
                    beta: 1.0,
                },
            )),
            None,
        )
        .get();

        let mut linear_scan = LinearScan::new();
        linear_scan.index(&data);
        let ls_result = linear_scan.run(&queries);

        let mut period_index = PeriodIndex::new(128, 4).unwrap();
        period_index.index(&data);
        let pi_result = period_index.run(&queries);

        for (idx, (ls_ans, pi_ans)) in ls_result.into_iter().zip(pi_result.into_iter()).enumerate()
        {
            let ls_ints: BTreeSet<Interval> = ls_ans.intervals().into_iter().collect();
            let pq_ints: BTreeSet<Interval> = pi_ans.intervals().into_iter().collect();
            assert_eq!(
                ls_ints,
                pq_ints,
                "query is {:?}\n\nmissing intervals: {:?} over {}, maximum length {:?}",
                queries[idx],
                ls_ints.difference(&pq_ints).count(),
                ls_ints.len(),
                ls_ints.difference(&pq_ints).map(|int| int.duration()).max()
            );
        }
    }

    #[test]
    fn test_crash_single() {
        pretty_env_logger::init();
        use std::collections::BTreeSet;
        let mut data = RandomDataset::new(
            123,
            10000,
            TimeDistribution::Uniform {
                low: 1,
                high: 10000,
            },
            TimeDistribution::Zipf {
                n: 1000000,
                beta: 1.0,
            },
        )
        .get();
        data.sort_unstable();
        data.dedup();
        let queries = vec![Query {
            range: Some(Interval {
                start: 8,
                end: 32114,
            }),
            duration: None,
        }];

        // let data = RandomDatasetZipfAndUniform::new(12351, 1000000, 1.0, 1000).get();

        let mut linear_scan = LinearScan::new();
        linear_scan.index(&data);
        let ls_result = linear_scan.run(&queries);

        let mut period_index = PeriodIndex::new(128, 4).unwrap();
        period_index.index(&data);
        let pi_result = period_index.run(&queries);

        for (idx, (ls_ans, pi_ans)) in ls_result.into_iter().zip(pi_result.into_iter()).enumerate()
        {
            let ls_ints: BTreeSet<Interval> = ls_ans.intervals().iter().copied().collect();
            let pq_ints: BTreeSet<Interval> = pi_ans.intervals().iter().copied().collect();
            assert!(
                ls_ints == pq_ints,
                "query is {:?}\nmissing intervals: {:?} over {}, maximum length {:?}\nfirst missing interval {:?}",
                queries[idx],
                ls_ints.difference(&pq_ints).count(),
                ls_ints.len(),
                ls_ints.difference(&pq_ints).map(|int| int.duration()).max(),
                ls_ints.difference(&pq_ints).map(|int| format!("{:?} {}", int, int.duration())).take(10).collect::<Vec<String>>()
            );
        }
    }
}
