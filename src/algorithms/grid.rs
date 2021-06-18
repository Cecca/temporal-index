use crate::types::*;
use std::collections::BTreeMap;
use std::ops::Bound::{Included, Unbounded};

pub struct Grid {
    num_buckets: usize,
    bucket_size: Option<usize>,
    /// Grid of intervals. Since the intervals can occupy only the upper triangle,
    /// we represent it as a vector of nested vectors in order not to waste space
    grid: Vec<Vec<Interval>>,
    start_time_ecdf: BTreeMap<Time, u32>,
    end_time_ecdf: BTreeMap<Time, u32>,
    n: Option<usize>,
}

impl std::fmt::Debug for Grid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "grid {:?}x{:?} for {:?} elements, bucket size {:?}",
            self.num_buckets, self.num_buckets, self.n, self.bucket_size
        )
    }
}

impl Grid {
    pub fn iter(&self) -> impl Iterator<Item = Interval> + '_ {
        self.grid.iter().flatten().copied()
    }

    fn ecdf<I: IntoIterator<Item = Time>>(times: I) -> BTreeMap<Time, u32> {
        let mut ecdf = BTreeMap::new();
        let mut n = 0;
        for t in times {
            ecdf.entry(t).and_modify(|c| *c += 1).or_insert(1);
            n += 1;
        }
        let mut cumulative_count = 0u32;
        for (_time, count) in ecdf.iter_mut() {
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

    fn index_for(&self, interval: Interval) -> (usize, usize) {
        let bucket_size = self.bucket_size.expect("non-configured index");
        let start_count = *self
            .start_time_ecdf
            .get(&interval.start)
            .expect("missing interval from the index");
        let end_count = *self
            .end_time_ecdf
            .get(&interval.end)
            .expect("missing interval from the index");
        (
            start_count as usize / bucket_size,
            end_count as usize / bucket_size,
        )
    }

    /// Gets the row major index in the grid, for a query (which requires transposing the endpoints)
    fn index_for_query(&self, interval: Interval) -> (usize, usize) {
        let bucket_size = self.bucket_size.expect("non-configured index");
        let start_count = self
            .start_time_ecdf
            .range((Included(interval.end), Unbounded))
            .next()
            .map(|pair| *pair.1)
            .unwrap_or(self.n.unwrap() as u32);
        let end_count = self
            .end_time_ecdf
            .range((Unbounded, Included(interval.start)))
            .next_back()
            .map(|pair| *pair.1)
            .unwrap_or(0);

        // the indices in the grid
        (
            start_count as usize / bucket_size,
            end_count as usize / bucket_size,
        )
    }

    pub fn new(num_buckets: usize) -> Self {
        Self {
            num_buckets,
            bucket_size: None,
            grid: Vec::new(),
            start_time_ecdf: BTreeMap::new(),
            end_time_ecdf: BTreeMap::new(),
            n: None,
        }
    }
}

impl Algorithm for Grid {
    fn name(&self) -> String {
        String::from("grid")
    }
    fn parameters(&self) -> String {
        format!("n_buckets={}", self.num_buckets)
    }
    fn version(&self) -> u8 {
        6
    }
    fn index(&mut self, dataset: &[Interval]) {
        self.clear();
        self.n.replace(dataset.len());
        self.start_time_ecdf = Self::ecdf(dataset.iter().map(|i| i.start));
        self.end_time_ecdf = Self::ecdf(dataset.iter().map(|i| i.end));

        self.bucket_size
            .replace(((dataset.len() / self.num_buckets as usize) as f64).ceil() as usize + 1);

        for _ in 0..(self.num_buckets * self.num_buckets) {
            self.grid.push(Vec::new());
        }

        let mut pl = progress_logger::ProgressLogger::builder()
            .with_items_name("intervals")
            .with_expected_updates(dataset.len() as u64)
            .start();
        for &interval in dataset {
            let idx = self.index_for(interval);
            self.grid[row_major(idx, self.num_buckets)].push(interval);
            pl.update_light(1u64);
        }
        pl.stop();
    }

    fn query(&self, query: &Query, answers: &mut QueryAnswerBuilder) {
        let grid_side = self.num_buckets;

        let mut cnt = 0;

        match (query.range, query.duration) {
            (Some(range), Some(duration)) => {
                let (qi, qj) = self.index_for_query(range);
                for i in 0..=qi {
                    for j in qj..grid_side {
                        self.grid[row_major((i, j), grid_side)]
                            .iter()
                            .for_each(|interval| {
                                cnt += 1;
                                let matches_duration = duration.contains(interval);
                                let overlaps = range.overlaps(interval);
                                if matches_duration && overlaps {
                                    answers.push(*interval);
                                }
                            });
                    }
                }
            }
            (None, Some(duration)) => {
                for i in 0..grid_side {
                    for j in 0..grid_side {
                        self.grid[row_major((i, j), grid_side)]
                            .iter()
                            .for_each(|interval| {
                                cnt += 1;
                                let matches_duration = duration.contains(interval);
                                if matches_duration {
                                    answers.push(*interval);
                                }
                            });
                    }
                }
            }
            (Some(range), None) => {
                let (qi, qj) = self.index_for_query(range);
                for i in 0..=qi {
                    for j in qj..grid_side {
                        self.grid[row_major((i, j), grid_side)]
                            .iter()
                            .for_each(|interval| {
                                cnt += 1;
                                let overlaps = range.overlaps(interval);
                                if overlaps {
                                    answers.push(*interval);
                                }
                            });
                    }
                }
            }
            (None, None) => {
                for i in 0..grid_side {
                    for j in 0..grid_side {
                        self.grid[row_major((i, j), grid_side)]
                            .iter()
                            .for_each(|interval| {
                                cnt += 1;
                                answers.push(*interval);
                            });
                    }
                }
            }
        }

        answers.inc_examined(cnt);
    }

    fn clear(&mut self) {
        self.grid.clear();
        self.start_time_ecdf.clear();
        self.end_time_ecdf.clear();
        self.n.take();
    }
}

#[inline]
fn row_major((i, j): (usize, usize), n_cols: usize) -> usize {
    i * n_cols + j
}
