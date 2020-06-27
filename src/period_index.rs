use crate::types::*;
use anyhow::{anyhow, Result};
use std::rc::{Rc, Weak};

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
        debug_assert!(self.duration_range.contains(&interval));
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
                // before the interval
                todo!("also check that the query hits a previous cell");
                if self.time_range.start <= interval.start {
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
        while duration > 0 {
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
        }

        Self { time_range, cells }
    }

    fn insert(&mut self, interval: Interval) {
        let d = interval.duration();
        let level = std::cmp::min(
            self.cells.len(),
            std::cmp::max(
                (self.time_range.duration() as f64 / d as f64)
                    .log2()
                    .floor() as usize,
                0,
            ),
        );

        for cell in self.cells[level].iter_mut() {
            cell.insert(interval);
        }
    }

    fn query<F: FnMut(&Interval)>(&self, query: &Query, action: &mut F) {
        let (level_min, level_max) = match query.duration {
            None => (0, self.cells.len()),
            Some(duration_range) => {
                let level_min = std::cmp::min(
                    self.cells.len(),
                    std::cmp::max(
                        (self.time_range.duration() as f64 / duration_range.max as f64)
                            .log2()
                            .floor() as usize,
                        0,
                    ),
                );
                let level_max = std::cmp::min(
                    self.cells.len(),
                    std::cmp::max(
                        (self.time_range.duration() as f64 / duration_range.min as f64)
                            .log2()
                            .floor() as usize,
                        0,
                    ),
                );
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
                "too many levels, should be less than log2(bucket_length)"
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
        todo!()
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

        self.buckets.clear();
        self.anchor_point = anchor;
        self.n = dataset.len();
        let mut start = anchor;
        while start < endtime {
            let bucket = Bucket::new(Interval::new(start, self.bucket_length), self.num_levels);
            self.buckets.push(bucket);
            start += self.bucket_length;
        }

        for interval in dataset {
            for bucket in self.buckets.iter_mut () {
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
                if query.range.map(|r| r.overlaps(&bucket.time_range)).unwrap_or(true) {
                    bucket.query(query, &mut |interval| {
                        ans_builder.push(*interval);
                    });
                }
            }
        }
        answers
    }
}
