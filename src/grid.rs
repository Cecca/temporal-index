use crate::types::*;
use deepsize::DeepSizeOf;
use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Included, Unbounded};

pub struct Grid {
    bucket_size: usize,
    grid: Vec<Vec<Interval>>,
    start_time_ecdf: BTreeMap<Time, u32>,
    end_time_ecdf: BTreeMap<Time, u32>,
    n: Option<usize>,
    num_rows: Option<usize>,
    num_columns: Option<usize>,
}

impl std::fmt::Debug for Grid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "grid {:?}x{:?} for {:?} elements", self.num_rows, self.num_columns, self.n)
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
        assert!(n == cumulative_count, "n is {}, while the cumulative count is {}", n, cumulative_count);
        ecdf
    }

    /// Gets the row major index in the grid
    fn index_for(&self, interval: Interval) -> (usize, usize) {
        assert!(self.num_columns.is_some(), "using uninitialized index");
        let num_columns = self.num_columns.unwrap();
        let (start_time, start_count) = self
            .start_time_ecdf
            .range((Unbounded, Included(interval.end)))
            .next_back()
            .expect("empty iterator");
        let (end_time, end_count) = self
            .end_time_ecdf
            .range((Included(interval.start), Unbounded))
            .next()
            .expect("empty iterator");

        // the indices in the grid
        (
            *start_count as usize / self.bucket_size,
            *end_count as usize / self.bucket_size,
        )
    }

    pub fn new(bucket_size: usize) -> Self {
        Self {
            bucket_size,
            grid: Vec::new(),
            start_time_ecdf: BTreeMap::new(),
            end_time_ecdf: BTreeMap::new(),
            n: None,
            num_rows: None,
            num_columns: None,
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
        let start_tot_count = *self
            .start_time_ecdf
            .iter()
            .next_back()
            .expect("empty ecdf")
            .1 as usize;
        let end_tot_count = *self.end_time_ecdf.iter().next_back().expect("empty ecdf").1 as usize;
        let num_columns = start_tot_count / self.bucket_size;
        let num_rows = end_tot_count / self.bucket_size;
        self.num_columns.replace(num_columns);
        self.num_rows.replace(num_rows);
        let max_idx =
            (start_tot_count / self.bucket_size) * num_columns + end_tot_count / self.bucket_size;
        debug!(
            "Grid with {} rows {} columns and {} cells overall",
            num_rows,
            num_columns,
            max_idx + 1
        );

        for i in 0..=max_idx {
            self.grid.push(Vec::new());
        }

        for &interval in dataset {
            let idx = self.index_for(interval);
            let idx = row_major(idx, self.num_columns.unwrap());
            self.grid[idx].push(interval);
        }
    }

    fn run(&self, queries: &[Query]) -> Vec<QueryAnswer> {
        let mut answers = Vec::with_capacity(queries.len());
        let n_rows = self.num_rows.unwrap();
        let n_cols = self.num_columns.unwrap();

        for query in queries {
            let mut ans_builder = QueryAnswer::builder(self.n.unwrap());

            let (qi, qj) = if let Some(qrange) = query.range {
                self.index_for(qrange)
            } else {
                (n_rows, 0)
            };
            println!("start cell ({},{}), {:?}", qi, qj, self);
            for i in 0..=qi {
                for j in qj..n_cols {
                    self.grid[row_major((i, j), n_cols)]
                        .iter()
                        .for_each(|interval| {
                            let matches_duration = query
                                .duration
                                .as_ref()
                                .map(|d| d.contains(interval))
                                .unwrap_or(true);
                            let overlaps = query
                                .range
                                .as_ref()
                                .map(|range| range.overlaps(interval))
                                .unwrap_or(true);
                            if matches_duration && overlaps {
                                ans_builder.push(*interval);
                            }
                        });
                }
            }

            answers.push(ans_builder.finalize());
        }
        answers
    }
    fn clear(&mut self) {
        self.grid.clear();
        self.start_time_ecdf.clear();
        self.end_time_ecdf.clear();
        self.n.take();
        self.num_rows.take();
        self.num_columns.take();
    }
}

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
        let data = RandomDatasetZipfAndUniform::new(12351, 1000000, 1.0, 1000).get();
        let queries = vec![Query {
            range: Some(Interval::new(18, 4)),
            duration: Some(DurationRange::new(1, 1)),
        }];

        let mut linear_scan = LinearScan::new();
        linear_scan.index(&data);
        let ls_result = linear_scan.run(&queries);

        let mut period_index = Grid::new(1280);
        period_index.index(&data);
        let pi_result = period_index.run(&queries);

        for (ls_ans, pi_ans) in ls_result.into_iter().zip(pi_result.into_iter()) {
            assert_eq!(ls_ans.intervals(), pi_ans.intervals());
        }
    }

    #[test]
    fn test_same_result() {
        let data = RandomDatasetZipfAndUniform::new(123, 10, 1.0, 1000).get();
        let queries = RandomQueriesZipfAndUniform::new(1734, 10, 1.0, 1000, 0.5).get();

        let mut linear_scan = LinearScan::new();
        linear_scan.index(&data);
        let ls_result = linear_scan.run(&queries);

        let mut period_index = Grid::new(128);
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
    }}
