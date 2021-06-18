use crate::types::*;

pub struct GridFile {
    side_cells: usize,
    /// index by duration (first dimension) and then by start time
    /// the end itme is correlated, so we don't build an index in that dimension
    inner: Vec<Vec<Vec<Interval>>>,
    duration_anchor: Option<Time>,
    start_anchor: Option<Time>,
    durations_per_cell: Option<usize>,
    starts_per_cell: Option<usize>,
    max_durations: Vec<Time>,
}

impl std::fmt::Debug for GridFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "grid-file({})", self.side_cells)
    }
}

impl GridFile {
    pub fn new(side_cells: usize) -> Self {
        Self {
            side_cells,
            inner: Vec::new(),
            duration_anchor: None,
            start_anchor: None,
            durations_per_cell: None,
            starts_per_cell: None,
            max_durations: Vec::new(),
        }
    }

    #[inline(always)]
    fn map_time(&self, t: Time) -> usize {
        let ns = self.starts_per_cell.unwrap();
        let start_anchor = self.start_anchor.unwrap();
        if t < start_anchor {
            0
        } else {
            std::cmp::min((t - start_anchor) as usize / ns, self.side_cells)
        }
    }

    #[inline(always)]
    fn map_duration(&self, d: Time) -> usize {
        let nd = self.durations_per_cell.unwrap();
        let duration_anchor = self.duration_anchor.unwrap();
        if d < duration_anchor {
            0
        } else {
            std::cmp::min((d - duration_anchor) as usize / nd, self.side_cells)
        }
    }

    fn query_range_duration<F: FnMut(&Interval)>(
        &self,
        range: Interval,
        duration: DurationRange,
        mut action: F,
    ) -> u32 {
        let d_start = self.map_duration(duration.min);
        let d_end = self.map_duration(duration.max);
        let s_end = self.map_time(range.end);

        let mut cnt = 0;
        for (i, row) in self.inner[d_start..=d_end].iter().enumerate() {
            let i = i + d_start;
            let s_start = if range.start < self.max_durations[i] {
                0
            } else {
                self.map_time(range.start - self.max_durations[i])
            };
            let s_start = std::cmp::min(s_start, s_end);
            for cell in row[s_start..=s_end].iter() {
                for interval in cell {
                    cnt += 1;
                    if range.overlaps(interval) && duration.contains(interval) {
                        action(interval);
                    }
                }
            }
        }
        cnt
    }

    fn query_range<F: FnMut(&Interval)>(&self, range: Interval, mut action: F) -> u32 {
        let s_end = self.map_time(range.end);

        let mut cnt = 0;
        for (i, row) in self.inner.iter().enumerate() {
            let s_start = if range.start < self.max_durations[i] {
                0
            } else {
                self.map_time(range.start - self.max_durations[i])
            };
            let s_start = std::cmp::min(s_start, s_end);
            for cell in row[s_start..=s_end].iter() {
                for interval in cell {
                    cnt += 1;
                    if range.overlaps(interval) {
                        action(interval);
                    }
                }
            }
        }
        cnt
    }

    fn query_duration<F: FnMut(&Interval)>(&self, duration: DurationRange, mut action: F) -> u32 {
        let d_start = self.map_duration(duration.min);
        let d_end = self.map_duration(duration.max);

        let mut cnt = 0;
        for row in self.inner[d_start..=d_end].iter() {
            for cell in row.iter() {
                for interval in cell {
                    cnt += 1;
                    if duration.contains(interval) {
                        action(interval);
                    }
                }
            }
        }
        cnt
    }
}

impl Algorithm for GridFile {
    fn name(&self) -> String {
        "grid-file".to_owned()
    }

    fn parameters(&self) -> String {
        format!("side_cells={}", self.side_cells)
    }

    fn version(&self) -> u8 {
        1
    }

    fn index(&mut self, dataset: &[Interval]) {
        self.clear();

        let side_cells = self.side_cells;
        self.inner.resize_with(side_cells + 1, || {
            let mut row = Vec::new();
            row.resize_with(side_cells + 1, || Vec::new());
            row
        });
        self.max_durations.resize(side_cells + 1, 0);

        let min_duration = dataset
            .iter()
            .min_by_key(|interval| interval.duration())
            .unwrap()
            .duration();
        let max_duration = dataset
            .iter()
            .max_by_key(|interval| interval.duration())
            .unwrap()
            .duration();
        let min_start_time = dataset
            .iter()
            .min_by_key(|interval| interval.start)
            .unwrap()
            .start;
        let max_start_time = dataset
            .iter()
            .max_by_key(|interval| interval.start)
            .unwrap()
            .start;

        self.duration_anchor.replace(min_duration);
        self.start_anchor.replace(min_start_time);
        let span_duration = max_duration - min_duration;
        self.durations_per_cell
            .replace((span_duration as f64 / side_cells as f64).ceil() as usize);
        let span_starts = max_start_time - min_start_time;
        self.starts_per_cell
            .replace((span_starts as f64 / side_cells as f64).ceil() as usize);

        let durations_per_cell = self.durations_per_cell.unwrap();
        let starts_per_cell = self.starts_per_cell.unwrap();
        for interval in dataset {
            let d_idx = self.map_duration(interval.duration());
            let s_idx = self.map_time(interval.start);
            assert!(d_idx < self.inner.len());
            assert!(
                s_idx < self.inner[d_idx].len(),
                "{} = {} / {} >= {}",
                s_idx,
                interval.start,
                starts_per_cell,
                self.inner[d_idx].len()
            );
            self.inner[d_idx][s_idx].push(interval.clone());
        }
        self.max_durations[0] = min_duration + durations_per_cell as Time;
        for i in 1..self.max_durations.len() {
            self.max_durations[i] = self.max_durations[i - 1] + durations_per_cell as Time;
        }
    }

    fn query(&self, query: &Query, answer: &mut QueryAnswerBuilder) {
        let examined = match (query.range, query.duration) {
            (Some(range), Some(duration)) => {
                self.query_range_duration(range, duration, |interval| answer.push(*interval))
            }
            (Some(range), None) => self.query_range(range, |interval| answer.push(*interval)),
            (None, Some(duration)) => {
                self.query_duration(duration, |interval| answer.push(*interval))
            }
            (None, None) => unimplemented!("Enumeration not supported"),
        };
        answer.inc_examined(examined);
    }

    fn clear(&mut self) {
        drop(std::mem::replace(&mut self.inner, Vec::new()));
        drop(std::mem::replace(&mut self.max_durations, Vec::new()));
        drop(self.starts_per_cell.take());
        drop(self.durations_per_cell.take());
        drop(self.duration_anchor.take());
        drop(self.start_anchor.take());
    }
}
