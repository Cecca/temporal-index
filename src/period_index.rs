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
    cells: Vec<Vec<Cell>>,
    n: u32,
}

impl Bucket {
    fn new(time_range: Interval, num_levels: u32) -> Self {
        let mut cells = Vec::with_capacity(num_levels as usize);
        let mut duration = time_range.duration();
        let mut duration_range = DurationRange::new(duration, std::u32::MAX);
        let mut level_count = 0;
        while duration > 0 && level_count < num_levels {
            trace!("level {}, duration range {:?}", level_count, duration_range);
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

        Self {
            time_range,
            cells,
            n: 0,
        }
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
        self.n += 1;
        assert!(cnt > 0);
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
            for cell in self.cells[level][start..=end].iter() {
                cnt += cell.query_range_duration(range, duration, action);
            }
        }
        cnt
    }

    fn query_range<F: FnMut(&Interval)>(&self, range: Interval, action: &mut F) -> u32 {
        let mut cnt = 0;
        for level in 0..self.cells.len() {
            let (start, end) = self.cells_for(level, range);
            for cell in self.cells[level][start..=end].iter() {
                cnt += cell.query_range_only(range, action);
            }
        }
        cnt
    }

    fn query_duration<F: FnMut(&Interval)>(&self, duration: DurationRange, action: &mut F) -> u32 {
        let level_min = self.level_for(duration.max);
        let level_max = self.level_for(duration.min);
        let mut cnt = 0;
        for level in level_min..=level_max {
            for cell in self.cells[level].iter() {
                cnt += cell.query_duration_only(&duration, action);
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
                    .filter(|cell| cell.intervals.is_empty())
                    .count()
            })
            .sum()
    }

    fn count_cells(&self) -> usize {
        self.cells.iter().map(|level| level.len()).sum()
    }
}

#[derive(DeepSizeOf)]
pub struct PeriodIndex {
    num_buckets: usize,
    num_levels: u32,
    configured_num_levels: u32,
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
            configured_num_levels: num_levels,
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
        format!("{},{}", self.num_buckets, self.configured_num_levels)
    }

    fn version(&self) -> u8 {
        9
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
            pl.update_light(1u64);
        }
        pl.stop();
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
        let (start, end) = query
            .range
            .map(|r| self.bucket_for(r))
            .unwrap_or_else(|| (0, self.buckets.len() - 1));
        debug!("Loooking at buckets from {} to {}", start, end);
        let end = if end >= self.buckets.len() {
            self.buckets.len() - 1
        } else {
            end
        };
        match (query.range, query.duration) {
            (Some(range), Some(duration)) => {
                for bucket in self.buckets[start..=end].iter() {
                    let cnt = bucket.query_range_duration(range, duration, &mut |interval| {
                        answer.push(*interval);
                    });
                    answer.inc_examined(cnt);
                }
            }
            (Some(range), None) => {
                for bucket in self.buckets[start..=end].iter() {
                    let cnt = bucket.query_range(range, &mut |interval| {
                        answer.push(*interval);
                    });
                    answer.inc_examined(cnt);
                }
            }
            (None, Some(duration)) => {
                for bucket in self.buckets[start..=end].iter() {
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
        self.bucket_length.take();
        self.n = 0;
    }
}

#[derive(DeepSizeOf)]
pub struct PeriodIndexStar {
    /// Buckets wof different widths, sorted by their end times so that we can
    /// make binary search for start times on them.
    buckets: Vec<Bucket>,
    num_buckets: usize,
    num_levels: u32,
    n: usize,
}

impl PeriodIndexStar {
    pub fn new(num_buckets: usize, num_levels: u32) -> Result<Self> {
        Ok(Self {
            num_buckets,
            num_levels,
            n: 0,
            buckets: Vec::new(),
        })
    }

    fn first_bucket_for(&self, interval: &Interval) -> usize {
        self.buckets
            // Offset by one because we are comparing start with end times
            .binary_search_by_key(&interval.start, |b| b.time_range.end - 1)
            .unwrap_or_else(|i| i)
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
        format!("{}-{}", self.num_buckets, self.num_levels)
    }

    fn version(&self) -> u8 {
        1
    }

    fn index(&mut self, dataset: &[Interval]) {
        self.clear();

        let t_max = dataset
            .iter()
            .map(|interval| interval.end)
            .max()
            .expect("empty dataset");

        let mut chronon_ecdf = vec![0; t_max as usize];

        // compute the distribution function
        for interval in dataset {
            for chronon in
                chronon_ecdf[(interval.start as usize)..(interval.end as usize)].iter_mut()
            {
                *chronon += 1u32;
            }
        }

        // compute ecdf
        for i in 1..chronon_ecdf.len() {
            chronon_ecdf[i] += chronon_ecdf[i - 1];
        }

        let total = chronon_ecdf[chronon_ecdf.len() - 1];
        let step = total / self.num_buckets as u32;
        let mut threshold = step;
        let mut prev_boundary = 0u32;

        for (i, &count) in chronon_ecdf.iter().enumerate() {
            if count > threshold {
                let bucket = Bucket::new(
                    Interval::new(prev_boundary, i as u32 - prev_boundary),
                    self.num_levels,
                );
                trace!(
                    "created bucket {:?} (d={})",
                    bucket.time_range,
                    bucket.time_range.duration()
                );
                prev_boundary = i as u32;
                threshold += step;
                self.buckets.push(bucket);
            }
        }

        for interval in dataset {
            let i = self.first_bucket_for(interval);
            for bucket in self.buckets[i..].iter_mut() {
                if !bucket.time_range.overlaps(interval) {
                    break;
                }
                bucket.insert(*interval);
            }
        }
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
        self.buckets.clear()
    }
}
