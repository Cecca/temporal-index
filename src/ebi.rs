use crate::types::*;
use deepsize::DeepSizeOf;
use std::collections::BTreeMap;

#[derive(DeepSizeOf)]
enum Endpoint {
    Start(Interval),
    End(Interval),
}

pub struct EBIIndex {
    endpoints: BTreeMap<Time, Vec<Endpoint>>,
}

impl Default for EBIIndex {
    fn default() -> Self {
        Self {
            endpoints: BTreeMap::new()
        }
    }
}  

impl std::fmt::Debug for EBIIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EBIIndex")
    }
}

impl DeepSizeOf for EBIIndex {
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        // I actually don't know the overhead of the BTReeMap
        self.endpoints.iter().fold(0, |sum, (key, val)| {
            sum + key.deep_size_of_children(context) + val.deep_size_of_children(context)
        })
    }
}

impl Algorithm for EBIIndex {
    fn name(&self) -> String {
        String::from("ebi-index")
    }
    fn parameters(&self) -> String {
        String::from("")
    }
    fn version(&self) -> u8 {
        1
    }
    fn index(&mut self, dataset: &[Interval]) {
        self.clear();

        for interval in dataset {
            self.endpoints
                .entry(interval.start)
                .or_insert_with(Vec::new)
                .push(Endpoint::Start(*interval));

            self.endpoints
                .entry(interval.end)
                .or_insert_with(Vec::new)
                .push(Endpoint::End(*interval));
        }
    }

    fn query(&self, query: &Query, answers: &mut QueryAnswerBuilder) {
        match (query.range, query.duration) {
            (Some(range), Some(duration)) => {
                for (_t, endpoints) in self.endpoints.range(range.start..range.end) {
                    for e in endpoints {
                        match e {
                            Endpoint::Start(interval) => {
                                if duration.contains(interval) {
                                    answers.push(*interval);
                                }
                            }
                            Endpoint::End(interval) => {
                                if interval.start < range.start && range.start < interval.end && duration.contains(interval) {
                                    answers.push(*interval);
                                }
                            }
                        }
                    }
                }
            }
            (Some(range), None) => {
                for (_t, endpoints) in self.endpoints.range(range.start..range.end) {
                    for e in endpoints {
                        match e {
                            Endpoint::Start(interval) => {
                                answers.push(*interval);
                            }
                            Endpoint::End(interval) => {
                                if interval.start < range.start && range.start < interval.end {
                                    answers.push(*interval);
                                }
                            }
                        }
                    }
                }
            }
            (None, Some(duration)) => {
                for (_t, endpoints) in self.endpoints.iter() {
                    for e in endpoints {
                        match e {
                            Endpoint::Start(interval) => {
                                if duration.contains(interval) {
                                    answers.push(*interval);
                                }
                            }
                            Endpoint::End(_) => {
                                // do nothing, report only once at start
                            }
                        }
                    }
                }
            }
            (None, None) => {
                for (_t, endpoints) in self.endpoints.iter() {
                    for e in endpoints {
                        match e {
                            Endpoint::Start(interval) => {
                                answers.push(*interval);
                            }
                            Endpoint::End(_) => {
                                // do nothing, report only once at start
                            }
                        }
                    }
                }
            }
        }
    }

    fn clear(&mut self) {
        self.endpoints.clear();
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use crate::dataset::*;
    use crate::naive::*;

    #[test]
    fn test_same_result_ebi_small() {
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

        let mut index = EBIIndex::default();
        index.index(&data);
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

        let mut period_index = EBIIndex::default();
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

        let mut period_index = EBIIndex::default();
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
