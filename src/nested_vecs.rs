use crate::types::*;
use deepsize::DeepSizeOf;

#[derive(DeepSizeOf)]
pub struct NestedVecs {
    durations: Vec<Time>,
    start_times: Vec<Vec<(Time, usize)>>,
}

impl NestedVecs {
    fn n_intervals(&self) -> usize {
        self.start_times
            .iter()
            .flat_map(|v| v.iter().map(|p| p.1))
            .sum()
    }
}

impl Default for NestedVecs {
    fn default() -> Self {
        Self {
            durations: Vec::new(),
            start_times: Vec::new(),
        }
    }
}

impl std::fmt::Debug for NestedVecs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NestedVecs over {} intervals", self.n_intervals())
    }
}

impl Algorithm for NestedVecs {
    fn name(&self) -> String {
        String::from("NestedVecs")
    }
    fn parameters(&self) -> String {
        String::new()
    }
    fn version(&self) -> u8 {
        2
    }
    fn index(&mut self, dataset: &[Interval]) {
        self.clear();
        let mut tmp = Vec::with_capacity(dataset.len());
        for interval in dataset {
            tmp.push((interval.duration(), interval.start));
        }

        // Sort by increasing duration and start time, lexicographically
        tmp.sort_unstable();

        // Accumulate the values with a single pass
        let mut last_d = tmp[0].0;
        let mut last_start = tmp[0].1;
        let mut count = 1usize;
        let mut current_vec = Vec::new();
        for (d, s) in tmp.drain(1..) {
            if d > last_d {
                current_vec.push((last_start, count));
                self.durations.push(last_d);
                self.start_times.push(current_vec);
                // reset state
                last_start = s;
                last_d = d;
                count = 1;
                current_vec = Vec::new();
            } else if s != last_start {
                current_vec.push((last_start, count));
                last_start = s;
                count = 1;
            } else {
                count += 1;
            }
        }
        current_vec.push((last_start, count));
        self.durations.push(last_d);
        self.start_times.push(current_vec);

        assert_eq!(self.durations.len(), self.start_times.len());
        assert_eq!(self.n_intervals(), dataset.len());

        let size = self.deep_size_of();
        info!(
            "Allocated for index: {} bytes ({} Mb)",
            size,
            size / (1024 * 1024)
        );
    }

    fn query(&self, query: &Query, answers: &mut QueryAnswerBuilder) {
        match (query.range, query.duration) {
            (Some(range), Some(duration)) => {
                let mut i = self
                    .durations
                    // there are no duplicates, so any match is unique
                    .binary_search(&duration.min)
                    .unwrap_or_else(|i| i);

                while i < self.durations.len() && self.durations[i] <= duration.max {
                    let d = self.durations[i];
                    if d >= duration.min {
                        let search_start = if d > range.start {
                            0
                        } else {
                            range.start - d + 1
                        };
                        let j = self.start_times[i]
                            .binary_search_by_key(&search_start, |p| p.0)
                            .unwrap_or_else(|i| i);
                        for &(s, c) in &self.start_times[i][j..] {
                            if s >= range.end {
                                break;
                            }
                            if s >= search_start {
                                let interval = Interval::new(s, d);
                                debug_assert!(range.overlaps(&interval));
                                for _ in 0..c {
                                    answers.push(interval);
                                }
                            }
                        }
                    }

                    i += 1;
                }
            }
            (Some(range), None) => {
                let mut i = 0;
                while i < self.durations.len() {
                    let d = self.durations[i];
                    let search_start = if d > range.start {
                        0
                    } else {
                        range.start - d + 1
                    };
                    let j = self.start_times[i]
                        .binary_search_by_key(&search_start, |p| p.0)
                        .unwrap_or_else(|i| i);
                    for &(s, c) in &self.start_times[i][j..] {
                        if s >= range.end {
                            break;
                        }
                        if s >= search_start {
                            let interval = Interval::new(s, d);
                            debug_assert!(range.overlaps(&interval));
                            for _ in 0..c {
                                answers.push(interval);
                            }
                        }
                    }

                    i += 1;
                }
            }
            (None, Some(duration)) => {
                let mut i = self
                    .durations
                    // there are no duplicates, so any match is unique
                    .binary_search(&duration.min)
                    .unwrap_or_else(|i| i);

                while i < self.durations.len() && self.durations[i] <= duration.max {
                    let d = self.durations[i];
                    if d >= duration.min {
                        for &(s, c) in &self.start_times[i] {
                            let interval = Interval::new(s, d);
                            debug_assert!(duration.contains(&interval));
                            for _ in 0..c {
                                answers.push(interval);
                            }
                        }
                    }

                    i += 1;
                }
            }
            (None, None) => todo!(),
        }
    }

    fn clear(&mut self) {
        self.durations.clear();
        self.start_times.clear();
    }
}