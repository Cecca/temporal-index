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

    pub fn query_range_duration<F: FnMut(usize, Interval)>(
        &self,
        range: Interval,
        duration: DurationRange,
        mut action: F,
    ) -> u32 {
        let mut cnt = 0;
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
                    cnt += c as u32;
                    if s >= range.end {
                        break;
                    }
                    if s >= search_start {
                        let interval = Interval::new(s, d);
                        debug_assert!(range.overlaps(&interval));
                        action(c, interval)
                    }
                }
            }

            i += 1;
        }
        cnt
    }

    pub fn query_range<F: FnMut(usize, Interval)>(&self, range: Interval, mut action: F) -> u32 {
        let mut cnt = 0u32;
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
                cnt += c as u32;
                if s >= range.end {
                    break;
                }
                if s >= search_start {
                    let interval = Interval::new(s, d);
                    debug_assert!(range.overlaps(&interval));
                    action(c, interval);
                }
            }

            i += 1;
        }
        cnt
    }

    pub fn query_duration<F: FnMut(usize, Interval)>(
        &self,
        duration: DurationRange,
        mut action: F,
    ) -> u32 {
        let mut cnt = 0u32;
        let mut i = self
            .durations
            // there are no duplicates, so any match is unique
            .binary_search(&duration.min)
            .unwrap_or_else(|i| i);

        while i < self.durations.len() && self.durations[i] <= duration.max {
            let d = self.durations[i];
            if d >= duration.min {
                for &(s, c) in &self.start_times[i] {
                    cnt += c as u32;
                    let interval = Interval::new(s, d);
                    debug_assert!(duration.contains(&interval));
                    action(c, interval);
                }
            }

            i += 1;
        }
        cnt
    }

    pub fn for_each<F: FnMut(usize, Interval)>(&self, mut action: F) {
        for (d, times) in self.durations.iter().zip(self.start_times.iter()) {
            for (start, c) in times {
                let interval = Interval::new(*start, *d);
                action(*c, interval);
            }
        }
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
        5
    }
    fn index(&mut self, dataset: &[Interval]) {
        self.clear();
        let mut tmp = Vec::with_capacity(dataset.len());
        let mut pl = progress_logger::ProgressLogger::builder()
            .with_items_name("intervals")
            .with_expected_updates(dataset.len() as u64)
            .start();
        info!("Inserting intervals");
        for interval in dataset {
            tmp.push((interval.duration(), interval.start));
            pl.update_light(1u64);
        }
        pl.stop();

        // Sort by increasing duration and start time, lexicographically
        tmp.sort_unstable();

        // Accumulate the values with a single pass
        let mut last_d = tmp[0].0;
        let mut last_start = tmp[0].1;
        let mut count = 1usize;
        let mut current_vec = Vec::new();
        info!("Storing intervals");
        let mut pl = progress_logger::ProgressLogger::builder()
            .with_items_name("intervals")
            .with_expected_updates(dataset.len() as u64)
            .start();
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
            pl.update_light(1u64);
        }
        pl.stop();
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
                let cnt = self.query_range_duration(range, duration, |c, interval| {
                    for _ in 0..c {
                        answers.push(interval);
                    }
                });
                answers.inc_examined(cnt);
            }
            (Some(range), None) => {
                let cnt = self.query_range(range, |c, interval| {
                    for _ in 0..c {
                        answers.push(interval);
                    }
                });
                answers.inc_examined(cnt);
            }
            (None, Some(duration)) => {
                let cnt = self.query_duration(duration, |c, interval| {
                    for _ in 0..c {
                        answers.push(interval);
                    }
                });
                answers.inc_examined(cnt);
            }
            (None, None) => {
                let mut cnt = 0u32;
                self.for_each(|c, interval| {
                    cnt += c as u32;
                    for _ in 0..c {
                        answers.push(interval);
                    }
                });
                answers.inc_examined(cnt);
            }
        }
    }

    fn clear(&mut self) {
        self.durations.clear();
        self.start_times.clear();
    }
}
