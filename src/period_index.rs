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
                if (self.time_range.start <= interval.start)
                    || (query.range.is_some()
                        && self.time_range.start <= query.range.unwrap().start)
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

        // let mut start = (((interval.start - self.time_range.start) as f64) / d as f64).floor()
        //     * (1 << level) as f64;
        // if start < 0.0 {
        //     start = 0.0;
        // }
        // let mut end = (((interval.end - self.time_range.start) as f64) / d as f64).floor()
        //     * (1 << level) as f64;
        // if end < 0.0 {
        //     end = 0.0;
        // }
        // let start = start as usize;
        // let end = end as usize;
        // for cell in self.cells[level][start..end].iter_mut() {
        //     cell.insert(interval);
        // }
        for cell in self.cells[level].iter_mut() {
            if cell.time_range.overlaps(&interval) {
                cell.insert(interval);
            }
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
            for (idx, cell) in self.cells[level].iter().enumerate() {
                cell.query(query, &mut |interval| {
                    println!(
                        "First occurrence of {:?} at level {}, cell {} ({:?})",
                        interval, level, idx, cell.time_range
                    );
                    action(interval);
                });
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
        let mut answers = Vec::with_capacity(queries.len());
        for query in queries {
            let mut ans_builder = QueryAnswer::builder(self.n);
            for (idx, bucket) in self.buckets.iter().enumerate() {
                if query
                    .range
                    .map(|r| r.overlaps(&bucket.time_range))
                    .unwrap_or(true)
                {
                    println!("Bucket {}, ({:?})", idx, bucket.time_range);
                    bucket.query(query, &mut |interval| {
                        ans_builder.push(*interval);
                    });
                }
            }
            answers.push(ans_builder.finalize());
        }
        answers
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::dataset::*;
    use crate::naive::*;
    use crate::types::*;

    #[test]
    fn test_same_result() {
        let data = RandomDatasetZipfAndUniform::new(123, 10, 1.0, 1000).get();
        let queries = RandomQueriesZipfAndUniform::new(1734, 10, 1.0, 1000, 0.5).get();

        let mut linear_scan = LinearScan::new();
        linear_scan.index(&data);
        let ls_result = linear_scan.run(&queries);

        let mut period_index = PeriodIndex::new(128, 4).unwrap();
        period_index.index(&data);
        let mut pi_result = period_index.run(&queries);

        for (ls_ans, pi_ans) in ls_result.into_iter().zip(pi_result.into_iter()) {
            assert_eq!(ls_ans.intervals(), pi_ans.intervals());
        }
    }
}
