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
            endpoints: BTreeMap::new(),
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
                                if interval.start < range.start
                                    && range.start < interval.end
                                    && duration.contains(interval)
                                {
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
