use crate::types::*;
use anyhow::{anyhow, Result};

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
        self.intervals.push(interval);
    }

    fn query<F: FnMut(&Interval)>(&self, query: &Query, action: &mut F) {
        for interval in &self.intervals {
            if query
                .duration
                .as_ref()
                .map(|d| d.contains(interval))
                .unwrap_or(true)
                && query
                    .range
                    .as_ref()
                    .map(|r| r.overlaps(interval))
                    .unwrap_or(true)
            {
                // Check that this is the first occurrence, which happens if the cell starts
                // before the interval. If the interval starts before the cell,
                // we have still to check if the query covers the previous cell,
                // otherwise it's the first occurrence for _this_ query
                if self.time_range.start <= interval.start {
                    action(interval);
                } else if query.range.is_some()
                    && self.time_range.start <= query.range.unwrap().start
                {
                    action(interval);
                }
            }
        }
    }
}

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

    fn insert(&mut self, interval: Interval) {
        let d = interval.duration();
        let level = self.level_for(d);

        // TODO: Insert only in the ones that can contain it.
        for cell in self.cells[level].iter_mut() {
            cell.insert(interval);
        }
    }

    fn query<F: FnMut(&Interval)>(&self, query: &Query, action: &mut F) {
        let (level_min, level_max) = match query.duration {
            None => (0, self.cells.len()),
            Some(duration_range) => {
                let level_min = self.level_for(duration_range.max);
                let level_max = self.level_for(duration_range.min);
                debug_assert!(level_min <= level_max);
                (level_min, level_max)
            }
        };

        for level in level_min..=level_max {
            for cell in self.cells[level].iter() {
                cell.query(query, action);
            }
        }
    }
}

pub struct PeriodIndex {
    bucket_length: Time,
    anchor_point: Time,
    num_levels: u32,
    n: usize,
    buckets: Vec<Bucket>,
}

impl PeriodIndex {
    pub fn new(bucket_length: Time, num_levels: u32) -> Result<Self> {
        if num_levels > ((bucket_length as f64).log2().floor() as u32) {
            Err(anyhow!(
                "too many levels, should be less than log2(bucket_length), i.e. <={}",
                (bucket_length as f64).log2()
            ))
        } else {
            Ok(Self {
                bucket_length,
                anchor_point: 0,
                num_levels,
                n: 0,
                buckets: Vec::new(),
            })
        }
    }
}

impl Algorithm for PeriodIndex {
    fn name(&self) -> String {
        String::from("period-index")
    }

    fn parameters(&self) -> String {
        format!("{},{}", self.bucket_length, self.num_levels)
    }

    fn version(&self) -> u8 {
        1
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
        debug!("Anchor {}, endtime {}", anchor, endtime);

        self.buckets.clear();
        self.anchor_point = anchor;
        self.n = dataset.len();
        let mut start = anchor;
        while start < endtime {
            let bucket = Bucket::new(Interval::new(start, self.bucket_length), self.num_levels);
            debug!("bucket with interval {:?}", bucket.time_range);
            self.buckets.push(bucket);
            start += self.bucket_length;
        }
        debug!("Created {} buckets", self.buckets.len());

        for interval in dataset {
            for bucket in self.buckets.iter_mut() {
                if interval.overlaps(&bucket.time_range) {
                    bucket.insert(*interval);
                }
            }
        }
    }

    fn run(&self, queries: &[Query]) -> Vec<QueryAnswer> {
        let answers = Vec::with_capacity(queries.len());
        for query in queries {
            let mut ans_builder = QueryAnswer::builder(self.n);
            for bucket in self.buckets.iter() {
                if query
                    .range
                    .map(|r| r.overlaps(&bucket.time_range))
                    .unwrap_or(true)
                {
                    bucket.query(query, &mut |interval| {
                        ans_builder.push(*interval);
                    });
                }
            }
        }
        answers
    }
}
