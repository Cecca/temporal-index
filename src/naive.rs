use crate::types::*;

pub struct LinearScan {
    dataset: Vec<Interval>,
}

impl LinearScan {
    pub fn new() -> Self {
        Self {
            dataset: Vec::new()
        }
    }
}

impl Algorithm for LinearScan {
    fn name(&self) -> String {
        "linear-scan".to_owned()
    }
    fn parameters(&self) -> String {
        String::new()
    }

    fn version(&self) -> u8 {
        1
    }

    fn index(&mut self, dataset: &[Interval]) {
        self.dataset.clear();
        self.dataset.extend(dataset.iter().cloned());
    }
    
    fn run(&self, queries: &[Query]) -> Vec<QueryAnswer> {
        let mut result = Vec::with_capacity(queries.len());
        for query in queries.iter() {
            let mut query_result = QueryAnswer::builder(self.dataset.len());
            for (i, interval) in self.dataset.iter().enumerate() {
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
            result.push(query_result.finalize())
        }
        result
    }
}
