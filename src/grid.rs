use crate::types::*;
use deepsize::DeepSizeOf;
use std::collections::BTreeMap;
use std::ops::Bound::{Included, Unbounded};

pub struct Grid {
    bucket_size: usize,
    /// Grid of intervals. Since the intervals can occupy only the upper triangle,
    /// we represent it as a vector of nested vectors in order not to waste space
    grid: Vec<Vec<Interval>>,
    start_time_ecdf: BTreeMap<Time, u32>,
    end_time_ecdf: BTreeMap<Time, u32>,
    n: Option<usize>,
    grid_side: Option<usize>,
}

impl std::fmt::Debug for Grid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "grid {:?}x{:?} for {:?} elements, bucket size {}",
            self.grid_side, self.grid_side, self.n, self.bucket_size
        )
    }
}

impl Grid {
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
        let start_count = *self
            .start_time_ecdf
            .get(&interval.start)
            .expect("missing interval from the index");
        let end_count = *self
            .end_time_ecdf
            .get(&interval.end)
            .expect("missing interval from the index");
        (
            start_count as usize / self.bucket_size,
            end_count as usize / self.bucket_size,
        )
    }

    /// Gets the row major index in the grid, for a query (which requires transposing the endpoints)
    fn index_for_query(&self, interval: Interval) -> (usize, usize) {
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
            start_count as usize / self.bucket_size,
            end_count as usize / self.bucket_size,
        )
    }

    pub fn new(bucket_size: usize) -> Self {
        Self {
            bucket_size,
            grid: Vec::new(),
            start_time_ecdf: BTreeMap::new(),
            end_time_ecdf: BTreeMap::new(),
            n: None,
            grid_side: None,
        }
    }
}

impl DeepSizeOf for Grid {
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        // I actually don't know the overhead of the BTReeMap
        self.start_time_ecdf.iter().fold(0, |sum, (key, val)| {
            sum + key.deep_size_of_children(context) + val.deep_size_of_children(context)
        }) + self.end_time_ecdf.iter().fold(0, |sum, (key, val)| {
            sum + key.deep_size_of_children(context) + val.deep_size_of_children(context)
        }) + self.grid.deep_size_of()
    }
}

impl Algorithm for Grid {
    fn name(&self) -> String {
        String::from("grid")
    }
    fn parameters(&self) -> String {
        format!("{}", self.bucket_size)
    }
    fn version(&self) -> u8 {
        1
    }
    fn index(&mut self, dataset: &[Interval]) {
        self.clear();
        self.n.replace(dataset.len());
        self.start_time_ecdf = Self::ecdf(dataset.iter().map(|i| i.start));
        self.end_time_ecdf = Self::ecdf(dataset.iter().map(|i| i.end));
        let grid_side = dataset.len() / self.bucket_size + 1;
        self.grid_side.replace(grid_side);

        for _ in 0..(grid_side * grid_side) {
            self.grid.push(Vec::new());
        }

        for &interval in dataset {
            let idx = self.index_for(interval);
            self.grid[row_major(idx, grid_side)].push(interval);
        }
    }

    fn query(&self, query: &Query, answers: &mut QueryAnswerBuilder) {
        let grid_side = self.grid_side.unwrap();

        match (query.range, query.duration) {
            (Some(range), Some(duration)) => {
                let (qi, qj) = self.index_for_query(range);
                for i in 0..=qi {
                    for j in qj..grid_side {
                        self.grid[row_major((i, j), grid_side)]
                            .iter()
                            .for_each(|interval| {
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
                                answers.push(*interval);
                            });
                    }
                }
            }
        }
    }

    fn clear(&mut self) {
        self.grid.clear();
        self.start_time_ecdf.clear();
        self.end_time_ecdf.clear();
        self.n.take();
        self.grid_side.take();
    }
}

#[inline]
fn row_major((i, j): (usize, usize), n_cols: usize) -> usize {
    i * n_cols + j
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::dataset::*;
    use crate::naive::*;

    #[test]
    fn test_same_result_grid_small() {
        let data = RandomDatasetZipfAndUniform::new(12351, 1000000, 1.0, 100).get();
        // let data = vec![
        //     Interval { start: 18, end: 19 },
        //     Interval { start: 19, end: 20 },
        //     Interval { start: 20, end: 21 },
        //     Interval { start: 21, end: 22 },
        // ];
        let queries = vec![Query {
            range: Some(Interval::new(18, 4)),
            duration: Some(DurationRange::new(1, 1)),
        }];

        let mut linear_scan = LinearScan::new();
        linear_scan.index(&data);
        let ls_result = linear_scan.run(&queries);

        let mut index = Grid::new(100000);
        index.index(&data);
        let index_result = index.run(&queries);
        println!("{:?}", index.grid);

        for (ls_ans, i_ans) in ls_result.into_iter().zip(index_result.into_iter()) {
            assert_eq!(ls_ans.intervals(), i_ans.intervals());
        }
    }

    #[test]
    fn test_same_result() {
        let data = RandomDatasetZipfAndUniform::new(123, 1000000, 1.0, 1000).get();
        let queries = RandomQueriesZipfAndUniform::new(1734, 10, 1.0, 1000, 0.5).get();

        let mut linear_scan = LinearScan::new();
        linear_scan.index(&data);
        let ls_result = linear_scan.run(&queries);

        let mut period_index = Grid::new(100000);
        period_index.index(&data);
        let pi_result = period_index.run(&queries);

        for (ls_ans, pi_ans) in ls_result.into_iter().zip(pi_result.into_iter()) {
            assert_eq!(ls_ans.intervals(), pi_ans.intervals());
        }
    }

    #[test]
    fn test_same_result_2() {
        let data = RandomDatasetZipfAndUniform::new(12351, 1000000, 1.0, 1000).get();
        let queries = RandomQueriesZipfAndUniform::new(123415, 5, 1.0, 1000, 0.4).get();

        let mut linear_scan = LinearScan::new();
        linear_scan.index(&data);
        let ls_result = linear_scan.run(&queries);

        let mut period_index = Grid::new(128);
        period_index.index(&data);
        let pi_result = period_index.run(&queries);

        for (idx, (ls_ans, pi_ans)) in ls_result.into_iter().zip(pi_result.into_iter()).enumerate()
        {
            assert_eq!(
                ls_ans.intervals(),
                pi_ans.intervals(),
                "query is {:?}",
                queries[idx]
            );
        }
    }
}
