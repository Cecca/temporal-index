use crate::types::*;

pub struct LinearScan;

impl Algorithm for LinearScan {
    fn name(&self) -> String {
        "Linear Scan".to_owned()
    }
    fn parameters(&self) -> String {
        String::new()
    }

    fn version(&self) -> u8 {
        1
    }

    fn run(&self, dataset: &[Interval], queries: &[Query]) -> Vec<Vec<usize>> {
        let mut result = Vec::with_capacity(queries.len());
        for query in queries.iter() {
            let mut query_result = Vec::with_capacity(dataset.len());
            for (i, interval) in dataset.iter().enumerate() {
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
                    query_result.push(i);
                }
            }
            result.push(query_result)
        }
        result
    }
}
