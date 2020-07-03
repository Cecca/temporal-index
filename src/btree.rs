use crate::types::*;
use deepsize::DeepSizeOf;
use std::collections::BTreeMap;

pub struct BTree {
    data: BTreeMap<Time, Vec<Interval>>,
}

impl BTree {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }
}

impl std::fmt::Debug for BTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BTree")
    }
}

impl Algorithm for BTree {
    fn name(&self) -> String {
        String::from("BTree")
    }
    fn parameters(&self) -> String {
        String::new()
    }
    fn version(&self) -> u8 {
        1
    }
    fn index(&mut self, dataset: &[Interval]) {
        self.data.clear();
        for interval in dataset {
            self.data
                .entry(interval.duration())
                .or_insert_with(|| Vec::new())
                .push(*interval);
        }
        let size = self.deep_size_of();
        info!(
            "Allocated for index: {} bytes ({} Mb)",
            size,
            size / (1024 * 1024)
        );
    }
    fn run(&self, queries: &[Query]) -> Vec<QueryAnswer> {
        let mut result = Vec::with_capacity(queries.len());
        for query in queries.iter() {
            let mut query_result = QueryAnswer::builder(self.data.len());
            if let Some(duration_range) = query.duration {
                for (_duration, intervals) in
                    self.data.range(duration_range.min..=duration_range.max)
                {
                    for interval in intervals {
                        debug_assert!(duration_range.contains(interval));
                        let overlaps = query
                            .range
                            .as_ref()
                            .map(|range| range.overlaps(interval))
                            .unwrap_or(true);
                        if overlaps {
                            query_result.push(*interval);
                        }
                    }
                }
            } else {
                for (_duration, intervals) in self.data.iter() {
                    for interval in intervals {
                        let overlaps = query
                            .range
                            .as_ref()
                            .map(|range| range.overlaps(interval))
                            .unwrap_or(true);
                        if overlaps {
                            query_result.push(*interval);
                        }
                    }
                }
            }
            result.push(query_result.finalize())
        }
        result
    }
}

impl deepsize::DeepSizeOf for BTree {
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        // I actually don't know the overhead of the BTReeMap
        self.data.iter().fold(0, |sum, (key, val)| {
            sum + key.deep_size_of_children(context) + val.deep_size_of_children(context)
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::dataset::*;
    use crate::naive::*;

    #[test]
    fn test_same_result_btree() {
        let data = RandomDatasetZipfAndUniform::new(123, 10, 1.0, 1000).get();
        let queries = RandomQueriesZipfAndUniform::new(1734, 10, 1.0, 1000, 0.5).get();

        let mut linear_scan = LinearScan::new();
        linear_scan.index(&data);
        let ls_result = linear_scan.run(&queries);

        let mut period_index = BTree::new();
        period_index.index(&data);
        let pi_result = period_index.run(&queries);

        for (ls_ans, pi_ans) in ls_result.into_iter().zip(pi_result.into_iter()) {
            assert_eq!(ls_ans.intervals(), pi_ans.intervals());
        }
    }

    #[test]
    fn test_same_result_btree2() {
        let data = RandomDatasetZipfAndUniform::new(12351, 1000000, 1.0, 1000).get();
        let queries = RandomQueriesZipfAndUniform::new(123415, 5, 1.0, 1000, 0.4).get();

        let mut linear_scan = LinearScan::new();
        linear_scan.index(&data);
        let ls_result = linear_scan.run(&queries);

        let mut period_index = BTree::new();
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
