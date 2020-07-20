use crate::grid::Grid;
use crate::types::*;
use deepsize::DeepSizeOf;
use std::collections::BTreeMap;
use std::ops::Bound::{Included, Unbounded};
use std::ops::RangeInclusive;

pub struct Grid3D {
    num_buckets: usize,
    bucket_size: Option<usize>,
    /// Grid of intervals. Since the intervals can occupy only the upper triangle,
    /// we represent it as a vector of nested vectors in order not to waste space
    grid: Vec<Grid>,
    duration_ecdf: BTreeMap<Time, u32>,
    n: Option<usize>,
}

impl std::fmt::Debug for Grid3D {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "3d-grid {:?}x{:?}x{:?} for {:?} elements, bucket size {:?}",
            self.num_buckets, self.num_buckets, self.num_buckets, self.n, self.bucket_size
        )
    }
}

impl Grid3D {
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

    fn index_for(&self, duration: Time) -> usize {
        let bucket_size = self.bucket_size.expect("non configured index");
        let duration_count = *self
            .duration_ecdf
            .get(&duration)
            .expect("missing interval from the index");
        duration_count as usize / bucket_size
    }

    /// Gets the row major index in the grid, for a query (which requires transposing the endpoints, while using the duration as is)
    fn index_for_query(&self, range: DurationRange) -> RangeInclusive<usize> {
        let bucket_size = self.bucket_size.expect("non configured index");
        let n = self.n.expect("uninitialized index");
        let start = *self
            .duration_ecdf
            .range((Unbounded, Included(range.min)))
            .next_back()
            .unwrap_or((&0, &0))
            .1 as usize
            / bucket_size;
        let end = *self
            .duration_ecdf
            .range((Included(range.max), Unbounded))
            .next()
            .unwrap_or((&0, &(n as u32)))
            .1 as usize
            / bucket_size;

        start..=end
    }

    pub fn new(num_buckets: usize) -> Self {
        Self {
            num_buckets,
            bucket_size: None,
            grid: Vec::new(),
            duration_ecdf: BTreeMap::new(),
            n: None,
        }
    }
}

impl DeepSizeOf for Grid3D {
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        // I actually don't know the overhead of the BTReeMap
        self.duration_ecdf.iter().fold(0, |sum, (key, val)| {
            sum + key.deep_size_of_children(context) + val.deep_size_of_children(context)
        }) + self.grid.deep_size_of()
    }
}

impl Algorithm for Grid3D {
    fn name(&self) -> String {
        String::from("grid3D")
    }
    fn parameters(&self) -> String {
        format!("{}", self.num_buckets)
    }
    fn version(&self) -> u8 {
        2
    }
    fn index(&mut self, dataset: &[Interval]) {
        self.clear();
        self.n.replace(dataset.len());
        self.duration_ecdf = Self::ecdf(dataset.iter().map(|i| i.duration()));
        self.bucket_size.replace(((dataset.len() / self.num_buckets as usize) as f64).ceil() as usize + 1);
        let mut parts = Vec::new();

        for _ in 0..self.num_buckets {
            self.grid.push(Grid::new(self.num_buckets));
            parts.push(Vec::new());
        }

        // TODO Instead of copying data, do it with sorting and slicing.
        for &interval in dataset {
            let idx = self.index_for(interval.duration());
            parts[idx].push(interval);
        }

        for i in 0..parts.len() {
            self.grid[i].index(&parts[i]);
        }
    }

    fn query(&self, query: &Query, answers: &mut QueryAnswerBuilder) {

        match (query.range, query.duration) {
            (Some(_range), Some(duration)) => {
                let indices = self.index_for_query(duration);
                for grid in &self.grid[indices] {
                    grid.query(query, answers);
                }
            }
            (None, Some(duration)) => {
                let indices = self.index_for_query(duration);
                for grid in &self.grid[indices] {
                    grid.iter().for_each(|interval| {
                        let matches_duration = duration.contains(&interval);
                        if matches_duration {
                            answers.push(interval);
                        }
                    })
                }
            }
            (Some(_range), None) => {
                for grid in &self.grid {
                    grid.query(query, answers);
                }
            }
            (None, None) => {
                for grid in &self.grid {
                    grid.iter().for_each(|interval| {
                        answers.push(interval);
                    })
                }
            }
        }
    }

    fn clear(&mut self) {
        self.grid.clear();
        self.duration_ecdf.clear();
        self.n.take();
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::dataset::*;
    use crate::naive::*;

    #[test]
    fn test_same_result_grid_small() {
        let data = RandomDatasetZipfAndUniform::new(12351, 1000000, 1.0, 100).get();
        let queries = vec![Query {
            range: Some(Interval::new(18, 4)),
            duration: Some(DurationRange::new(1, 1)),
        }];

        let mut linear_scan = LinearScan::new();
        linear_scan.index(&data);
        let ls_result = linear_scan.run(&queries);

        let mut index = Grid3D::new(4);
        index.index(&data);
        println!("{:?}", index);
        let index_result = index.run(&queries);

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

        let mut period_index = Grid3D::new(4);
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

        let mut period_index = Grid3D::new(4);
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
