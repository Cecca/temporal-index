use crate::types::*;
use anyhow::Result;
use deepsize::DeepSizeOf;
use std::collections::BTreeMap;

#[derive(DeepSizeOf)]
struct Cell {
    time_range: Interval,
    duration_range: DurationRange,
    intervals: Vec<Interval>,
    /// index of the next non empty cell in the level
    next_nonempty: Option<usize>,
}

impl Cell {
    fn new(time_range: Interval, duration_range: DurationRange) -> Self {
        Self {
            time_range,
            duration_range,
            intervals: Vec::new(),
            next_nonempty: None,
        }
    }

    // fn set_next(&mut self, idx: usize) {
    //     self.next_nonempty.replace(idx);
    // }

    // fn is_empty(&self) -> bool {
    //     self.intervals.is_empty()
    // }

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
    ) -> u32 {
        let mut cnt = 0;
        for interval in &self.intervals {
            cnt += 1;
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
        cnt
    }

    fn query_range_only<F: FnMut(&Interval)>(&self, range: Interval, action: &mut F) -> u32 {
        let mut cnt = 0;
        for interval in &self.intervals {
            cnt += 1;
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
        cnt
    }

    fn query_duration_only<F: FnMut(&Interval)>(
        &self,
        duration: &DurationRange,
        action: &mut F,
    ) -> u32 {
        let mut cnt = 0;
        for interval in &self.intervals {
            cnt += 1;
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
        cnt
    }
}

#[derive(DeepSizeOf)]
struct Bucket {
    time_range: Interval,
    /// Two dimensional arrangement of cells: each level holds cells
    /// for intervals of different duration
    cells: Vec<Vec<(usize, Cell)>>,
    duration_ranges: Vec<DurationRange>,
    durations: Vec<Time>,
    n: u32,
}

// impl DeepSizeOf for Bucket {
//     fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
//         let btree_size = |btree: &BTreeMap<usize, Cell>| {
//             btree.iter().fold(0, |sum, (key, val)| {
//                 sum + key.deep_size_of_children(context) + val.deep_size_of_children(context)
//             })
//         };
//         self.cells.iter().map(btree_size).sum()
//     }
// }

// impl DeepSizeOf for Bucket {
//     fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
//         let btree_size = |btree: &BTreeMap<usize, Cell>| {
//             btree.iter().fold(0, |sum, (key, val)| {
//                 sum + key.deep_size_of_children(context) + val.deep_size_of_children(context)
//             })
//         };
//         self.cells.iter().map(btree_size).sum()
//     }
// }

impl Bucket {
    fn new(time_range: Interval, num_levels: u32) -> Self {
        let mut cells = Vec::with_capacity(num_levels as usize);
        let mut duration_ranges = Vec::new();
        let mut durations = Vec::new();
        let mut duration = time_range.duration();
        let mut duration_range = DurationRange::new(duration, std::u32::MAX);
        let mut level_count = 0;
        while duration > 0 && level_count < num_levels {
            trace!("level {}, duration range {:?}", level_count, duration_range);
            duration_ranges.push(duration_range);
            durations.push(duration);
            let level = Vec::new();
            cells.push(level);
            duration /= 2;
            duration_range.max = duration_range.min;
            duration_range.min = duration;
            level_count += 1;
        }

        // adjust lower bound on duration for the last level
        let last_idx = duration_ranges.len() - 1;
        duration_ranges[last_idx].min = 1;

        Self {
            time_range,
            cells,
            duration_ranges,
            durations,
            n: 0,
        }
    }

    // /// Set the indices to skip empty cells across all the levels
    // fn fix_levels(&mut self) {
    //     for level in self.cells.iter_mut() {
    //         let mut next_nonempty: Option<usize> = None;
    //         for i in (0..level.len()).rev() {
    //             if let Some(idx) = next_nonempty {
    //                 level[i].set_next(idx);
    //             }
    //             if !level[i].is_empty() {
    //                 next_nonempty.replace(i);
    //             }
    //         }
    //     }
    // }

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
        let cell_duration = self.durations[level];
        let start = if interval.start < self.time_range.start {
            0
        } else {
            ((interval.start - self.time_range.start) / cell_duration) as usize
        };
        // let end = if interval.end > self.time_range.end {
        //     self.cells[level].len() - 1
        // } else {
        //     ((interval.end - self.time_range.start) / cell_duration) as usize
        // };
        let end = ((interval.end - self.time_range.start) / cell_duration) as usize;
        (start, end)
    }

    fn insert(&mut self, interval: Interval) {
        let level = self.level_for(interval.duration());
        let duration_range_for_level = self.duration_ranges[level];
        let duration_for_level = self.durations[level];
        let (start, end) = self.cells_for(level, interval);

        // let mut cnt = 0;
        for idx in start..=end {
            let cell_time_range = Interval::new(
                self.time_range.start + idx as u32 * duration_for_level,
                duration_for_level,
            );
            if cell_time_range.overlaps(&interval) {
                // let cell = self.cells[level]
                //     .entry(idx)
                //     .or_insert_with(|| Cell::new(cell_time_range, duration_range_for_level));
                // cell.insert(interval);
                match self.cells[level].binary_search_by_key(&idx, |p| p.0) {
                    Ok(i) => {
                        let cell = &mut self.cells[level][i].1;
                        cell.insert(interval);
                    }
                    Err(i) => {
                        // create the cell, which was previously empty
                        let mut cell = Cell::new(cell_time_range, duration_range_for_level);
                        cell.insert(interval);
                        self.cells[level].insert(i, (idx, cell));
                    }
                }
            }
        }
        self.n += 1;
        // assert!(cnt > 0);
    }

    fn query_range_duration<F: FnMut(&Interval)>(
        &self,
        range: Interval,
        duration: DurationRange,
        action: &mut F,
    ) -> u32 {
        let level_min = self.level_for(duration.max);
        let level_max = self.level_for(duration.min);
        debug_assert!(level_min <= level_max);

        let mut cnt = 0;
        for level in level_min..=level_max {
            let (start, end) = self.cells_for(level, range);
            let mut i = self.cells[level]
                .binary_search_by_key(&start, |p| p.0)
                .or_else::<usize, _>(|i: usize| Ok(i))
                .unwrap();
            let cells = &self.cells[level];
            // for (_idx, cell) in cells.range(start..=end) {
            //     cnt += cell.query_range_duration(range, duration, action);
            // }
            while i < cells.len() && cells[i].0 <= end {
                let cell = &cells[i].1;
                cnt += cell.query_range_duration(range, duration, action);
                i += 1;
            }
        }
        cnt
    }

    fn query_range<F: FnMut(&Interval)>(&self, range: Interval, action: &mut F) -> u32 {
        let mut cnt = 0;
        for level in 0..self.cells.len() {
            let (start, end) = self.cells_for(level, range);
            let cells = &self.cells[level];
            let mut i = self.cells[level]
                .binary_search_by_key(&start, |p| p.0)
                .or_else::<usize, _>(|i: usize| Ok(i))
                .unwrap();
            // for (_idx, cell) in cells.range(start..=end) {
            //     cnt += cell.query_range_only(range, action);
            // }
            while i < cells.len() && cells[i].0 <= end {
                let cell = &cells[i].1;
                cnt += cell.query_range_only(range, action);
                i += 1;
            }
        }
        cnt
    }

    fn query_duration<F: FnMut(&Interval)>(&self, duration: DurationRange, action: &mut F) -> u32 {
        let level_min = self.level_for(duration.max);
        let level_max = self.level_for(duration.min);
        let mut cnt = 0;
        for level in level_min..=level_max {
            let cells = &self.cells[level];
            // for (_idx, cell) in cells {
            //     cnt += cell.query_duration_only(&duration, action);
            // }
            let mut i = 0;
            while i < cells.len() {
                let cell = &cells[i].1;
                cnt += cell.query_duration_only(&duration, action);
                i += 1;
                // if let Some(next) = cell.next_nonempty {
                //     i = next;
                // } else {
                //     break;
                // }
            }
        }
        cnt
    }

    fn count_empty_cells(&self) -> usize {
        self.cells
            .iter()
            .map(|level| {
                level
                    .iter()
                    .filter(|cell| cell.1.intervals.is_empty())
                    .count()
            })
            .sum()
    }

    fn count_cells(&self) -> usize {
        self.cells.iter().map(|level| level.len()).sum()
    }

    fn count_intervals(&self) -> usize {
        self.cells
            .iter()
            .map(|level| level.iter().map(|cs| cs.1.intervals.len()).sum::<usize>())
            .sum()
    }
}

#[derive(DeepSizeOf)]
pub struct PeriodIndex {
    num_buckets: usize,
    num_levels: u32,
    configured_num_levels: u32,
    anchor_point: Time,
    // The time spanned by the period index
    span: Option<Interval>,
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
            configured_num_levels: num_levels,
            anchor_point: 0,
            span: None,
            bucket_length: None,
            n: 0,
            buckets: Vec::new(),
        })
    }

    #[inline]
    fn bucket_for(&self, interval: Interval) -> Option<(usize, usize)> {
        // debug_assert!(interval.start >= self.anchor_point);
        if !self.span.expect("uninitialized index").overlaps(&interval) {
            return None;
        }
        let start = if interval.start < self.anchor_point {
            0
        } else {
            ((std::cmp::max(0, interval.start - self.anchor_point))
                / self.bucket_length.expect("uninitialized index")) as usize
        };
        let end = if interval.end < self.anchor_point {
            0
        } else {
            ((std::cmp::max(0, interval.end - self.anchor_point))
                / self.bucket_length.expect("uninitialized index")) as usize
        };
        Some((start, end))
    }
}

impl Algorithm for PeriodIndex {
    fn name(&self) -> String {
        String::from("period-index")
    }

    fn parameters(&self) -> String {
        format!(
            "n_buckets={} max_levels={}",
            self.num_buckets, self.configured_num_levels
        )
    }

    fn version(&self) -> u8 {
        18
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

        if self.configured_num_levels > ((bucket_length as f64).log2().floor() as u32) {
            warn!("the parameters `num_levels` is larger than the actual number of levels, not using it.");
            self.num_levels = (bucket_length as f64).log2().floor() as u32;
        } else {
            self.num_levels = self.configured_num_levels;
        }

        self.buckets.clear();
        self.anchor_point = anchor;
        self.span.replace(Interval {
            start: anchor,
            end: endtime,
        });
        self.n = dataset.len();
        let mut start = anchor;
        while start < endtime {
            let bucket = Bucket::new(Interval::new(start, bucket_length), self.num_levels);
            debug!("bucket with interval {:?}", bucket.time_range);
            self.buckets.push(bucket);
            start += bucket_length;
        }
        debug!("Created {} buckets", self.buckets.len());

        let mut pl = progress_logger::ProgressLogger::builder()
            .with_items_name("intervals")
            .with_expected_updates(dataset.len() as u64)
            .start();
        for interval in dataset {
            let (start, end) = self
                .bucket_for(*interval)
                .expect("during indexing, every intereval should fall in a bucket");
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
            pl.update_light(1u64);
        }
        pl.stop();
        info!("Setting jump pointers");
        // for bucket in self.buckets.iter_mut() {
        //     bucket.fix_levels();
        // }
        let size = self.deep_size_of();
        let empty_cells: usize = self.buckets.iter().map(|b| b.count_empty_cells()).sum();
        let num_cells: usize = self.buckets.iter().map(|b| b.count_cells()).sum();
        info!(
            "Allocated for index: {} bytes ({} Mb) - {} buckets ({}/{} empty cells)",
            size,
            size / (1024 * 1024),
            self.buckets.len(),
            empty_cells,
            num_cells
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
        match (query.range, query.duration) {
            (Some(range), Some(duration)) => {
                if let Some((start, end)) = self.bucket_for(range) {
                    let end = std::cmp::min(end, self.buckets.len() - 1);
                    for bucket in self.buckets[start..=end].iter() {
                        let cnt = bucket.query_range_duration(range, duration, &mut |interval| {
                            answer.push(*interval);
                        });
                        answer.inc_examined(cnt);
                    }
                }
            }
            (Some(range), None) => {
                if let Some((start, end)) = self.bucket_for(range) {
                    let end = std::cmp::min(end, self.buckets.len() - 1);
                    for bucket in self.buckets[start..=end].iter() {
                        let cnt = bucket.query_range(range, &mut |interval| {
                            answer.push(*interval);
                        });
                        answer.inc_examined(cnt);
                    }
                }
            }
            (None, Some(duration)) => {
                for bucket in self.buckets.iter() {
                    let cnt = bucket.query_duration(duration, &mut |interval| {
                        answer.push(*interval);
                    });
                    answer.inc_examined(cnt);
                }
            }
            (None, None) => {
                unimplemented!("iteration not supported");
            }
        }
    }

    fn clear(&mut self) {
        self.buckets.clear();
        self.anchor_point = 0;
        self.span.take();
        self.bucket_length.take();
        self.n = 0;
    }

    fn reporter_hook(&self, reporter: &crate::reporter::Reporter) -> anyhow::Result<()> {
        let bucket_info = self
            .buckets
            .iter()
            .enumerate()
            .map(|(index, bucket)| (index, bucket.count_intervals()));
        reporter.report_period_index_buckets(bucket_info)
    }
}

#[derive(DeepSizeOf)]
pub struct PeriodIndexStar {
    /// Buckets wof different widths, sorted by their end times so that we can
    /// make binary search for start times on them.
    buckets: Vec<Bucket>,
    /// Vector of bucket endpoints to make the search faster, since it fits into a cache line
    boundaries: Vec<u32>,
    num_buckets: u32,
    num_levels: u32,
    n: usize,
}

impl PeriodIndexStar {
    pub fn new(num_buckets: u32, num_levels: u32) -> Result<Self> {
        Ok(Self {
            num_buckets,
            num_levels,
            n: 0,
            buckets: Vec::new(),
            boundaries: Vec::new(),
        })
    }

    fn first_bucket_for(&self, interval: &Interval) -> usize {
        // on small vectors, linear search is faster than
        // binary search because of cache effects
        let mut i = 0;
        while i < self.boundaries.len() {
            if self.boundaries[i] > interval.start {
                return i;
            }
            i += 1;
        }
        self.boundaries.len() - 1
    }
}

impl std::fmt::Debug for PeriodIndexStar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "period-index-*({}, {})",
            self.num_buckets, self.num_levels
        )
    }
}

impl Algorithm for PeriodIndexStar {
    fn name(&self) -> String {
        String::from("period-index-*")
    }

    fn parameters(&self) -> String {
        format!(
            "n_buckets={} max_levels={}",
            self.num_buckets, self.num_levels
        )
    }

    fn version(&self) -> u8 {
        12
    }

    fn index(&mut self, dataset: &[Interval]) {
        self.clear();

        let start_times_ecdf: Vec<u32> = ecdf(dataset.iter().map(|interval| interval.start));
        let max_time = dataset.iter().map(|interval| interval.end).max().unwrap();

        let step = dataset.len() as u32 / self.num_buckets;
        let mut count_threshold = step;
        let mut last_time = 0;
        for (time, &count) in start_times_ecdf.iter().enumerate() {
            let time = time as u32;
            if count >= count_threshold {
                let bucket = Bucket::new(
                    Interval {
                        start: last_time,
                        end: time,
                    },
                    self.num_levels,
                );
                self.boundaries.push(bucket.time_range.end);
                self.buckets.push(bucket);
                last_time = time;
                count_threshold += step;
            }
        }
        // push the last bucket
        let bucket = Bucket::new(
            Interval {
                start: last_time,
                end: max_time,
            },
            self.num_levels,
        );
        self.boundaries.push(bucket.time_range.end);
        self.buckets.push(bucket);

        info!("Start inserting intervals");
        let mut pl = progress_logger::ProgressLogger::builder()
            .with_items_name("intervals")
            .with_expected_updates(dataset.len() as u64)
            .start();
        for interval in dataset {
            let mut idx = self.first_bucket_for(interval);
            while idx < self.buckets.len() && self.buckets[idx].time_range.overlaps(interval) {
                self.buckets[idx].insert(*interval);
                idx += 1;
            }
            pl.update_light(1u64);
        }
        pl.stop();
        info!("Setting jump pointers");
        // for bucket in self.buckets.iter_mut() {
        //     bucket.fix_levels();
        // }

        let size = self.deep_size_of();
        let empty_cells: usize = self.buckets.iter().map(|b| b.count_empty_cells()).sum();
        let num_cells: usize = self.buckets.iter().map(|b| b.count_cells()).sum();
        let perc_empty = (empty_cells as f64) / (num_cells as f64) * 100.0;
        info!(
            "Allocated for index: {} bytes ({} Mb) - {} buckets - {} empty over {} cells ({:.2}%)",
            size,
            size / (1024 * 1024),
            self.buckets.len(),
            empty_cells,
            num_cells,
            perc_empty
        );
    }

    fn query(&self, query: &Query, answer: &mut QueryAnswerBuilder) {
        match (query.range, query.duration) {
            (Some(range), Some(duration)) => {
                let start = self.first_bucket_for(&range);
                for bucket in self.buckets[start..].iter() {
                    if !bucket.time_range.overlaps(&range) {
                        break;
                    }
                    let cnt = bucket.query_range_duration(range, duration, &mut |interval| {
                        answer.push(*interval);
                    });
                    answer.inc_examined(cnt);
                }
            }
            (Some(range), None) => {
                let start = self.first_bucket_for(&range);
                for bucket in self.buckets[start..].iter() {
                    if !bucket.time_range.overlaps(&range) {
                        break;
                    }
                    let cnt = bucket.query_range(range, &mut |interval| {
                        answer.push(*interval);
                    });
                    answer.inc_examined(cnt);
                }
            }
            (None, Some(duration)) => {
                for bucket in self.buckets.iter() {
                    let cnt = bucket.query_duration(duration, &mut |interval| {
                        answer.push(*interval);
                    });
                    answer.inc_examined(cnt);
                }
            }
            (None, None) => {
                unimplemented!("iteration not supported");
            }
        }
    }

    fn clear(&mut self) {
        self.buckets.clear();
        self.boundaries.clear();
    }

    fn reporter_hook(&self, reporter: &crate::reporter::Reporter) -> anyhow::Result<()> {
        let bucket_info = self
            .buckets
            .iter()
            .enumerate()
            .map(|(index, bucket)| (index, bucket.count_intervals()));
        reporter.report_period_index_buckets(bucket_info)
    }
}

fn ecdf<I: IntoIterator<Item = Time>>(times: I) -> Vec<u32> {
    let mut ecdf = Vec::new();
    let mut n = 0;
    for t in times {
        if t as usize >= ecdf.len() {
            ecdf.resize(t as usize + 1, 0);
        }
        ecdf[t as usize] += 1u32;
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
