use crate::types::*;
use deepsize::DeepSizeOf;

#[derive(DeepSizeOf, Debug, Clone, Copy)]
pub enum DimensionOrder {
    TimeDuration,
    DurationTime,
}

#[derive(DeepSizeOf)]
pub struct RDIndex {
    dimension_order: DimensionOrder,
    page_size: usize,
    /// This inner data structure abstracts the choices induced by the order of the dimensions
    grid: Option<Grid>,
}

impl std::fmt::Debug for RDIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "rd-index({:?}, {})",
            self.dimension_order, self.page_size
        )
    }
}

impl RDIndex {
    pub fn new(dimension_order: DimensionOrder, page_size: usize) -> Self {
        Self {
            dimension_order,
            page_size,
            grid: None,
        }
    }
}

impl Algorithm for RDIndex {
    fn name(&self) -> String {
        "rd-index".to_owned()
    }

    fn parameters(&self) -> String {
        format!(
            "dimension_order={:?} page_size={}",
            self.dimension_order, self.page_size
        )
    }

    fn version(&self) -> u8 {
        1
    }

    fn index(&mut self, dataset: &[Interval]) {
        use std::iter::FromIterator;
        let mut dataset = Vec::from_iter(dataset.iter().cloned());
        self.grid.replace(Grid::new(
            &mut dataset,
            self.page_size,
            self.dimension_order,
        ));
    }

    fn query(&self, query: &Query, answer: &mut QueryAnswerBuilder) {
        if let Some(grid) = &self.grid {
            let examined = match (query.range, query.duration) {
                (Some(range), Some(duration)) => {
                    grid.query_range_duration(range, duration, |interval| answer.push(*interval))
                }
                (Some(range), None) => grid.query_range(range, |interval| answer.push(*interval)),
                (None, Some(duration)) => {
                    grid.query_duration(duration, |interval| answer.push(*interval))
                }
                (None, None) => unimplemented!("iteration is not supported"),
            };
            answer.inc_examined(examined);
        } else {
            panic!("uninitialized index!");
        }

        todo!()
    }

    fn clear(&mut self) {
        drop(self.grid.take());
    }
}

fn next_breakpoint<F: Fn(&Interval) -> Time>(
    intervals: &[Interval],
    start: usize,
    block: usize,
    key: F,
) -> usize {
    if start + block > intervals.len() {
        return intervals.len() - 1;
    }

    let lookahead = start + block;
    let lookahead_key = key(&intervals[lookahead]);
    let mut end = lookahead;
    if key(&intervals[start]) == lookahead_key {
        // We are looking at a heavy column/cell
        while end < intervals.len() && lookahead_key == key(&intervals[end + 1]) {
            end += 1;
        }
    } else {
        while end > start && key(&intervals[end]) == key(&intervals[end + 1]) {
            end -= 1;
        }
    }
    end
}
#[derive(DeepSizeOf)]
enum Grid {
    TimeDuration(TimePartition<DurationPartition<Vec<Interval>>>),
    DurationTime(DurationPartition<TimePartition<Vec<Interval>>>),
}

impl Grid {
    fn new(intervals: &mut [Interval], page_size: usize, order: DimensionOrder) -> Self {
        use std::iter::FromIterator;

        match order {
            DimensionOrder::TimeDuration => Self::TimeDuration(TimePartition::new(
                intervals,
                page_size * page_size,
                |column| {
                    DurationPartition::new(column, page_size, |cell| {
                        cell.sort_unstable_by_key(|interval| interval.end);
                        Vec::from_iter(cell.iter().cloned())
                    })
                },
            )),
            DimensionOrder::DurationTime => Self::DurationTime(DurationPartition::new(
                intervals,
                page_size * page_size,
                |column| {
                    TimePartition::new(column, page_size, |cell| {
                        cell.sort_unstable_by_key(|interval| interval.end);
                        Vec::from_iter(cell.iter().cloned())
                    })
                },
            )),
        }
    }

    #[allow(dead_code)]
    fn query_range_duration<F: FnMut(&Interval)>(
        &self,
        range: Interval,
        duration: DurationRange,
        mut action: F,
    ) -> u32 {
        let mut examined = 0;
        let mut cell_callback = |cell: &Vec<Interval>| {
            // traverse the cell by decreasing end time
            for interval in cell.iter().rev() {
                examined += 1;
                if interval.end <= range.start {
                    return;
                }
                if duration.contains(interval) && interval.start < range.end {
                    action(interval);
                }
            }
        };
        match self {
            Self::TimeDuration(grid) => {
                grid.query(range, |column| {
                    column.query(duration, &mut cell_callback);
                });
            }
            Self::DurationTime(grid) => grid.query(duration, |column| {
                column.query(range, &mut cell_callback);
            }),
        }
        examined
    }

    #[allow(dead_code)]
    fn query_range<F: FnMut(&Interval)>(&self, range: Interval, mut action: F) -> u32 {
        let mut examined = 0;
        let mut cell_callback = |cell: &Vec<Interval>| {
            // traverse the cell by decreasing end time
            for interval in cell.iter().rev() {
                examined += 1;
                if interval.end <= range.start {
                    return;
                }
                if interval.start < range.end {
                    action(interval);
                }
            }
        };
        match self {
            Self::TimeDuration(grid) => {
                grid.query(range, |column| {
                    column.for_each(&mut cell_callback);
                });
            }
            Self::DurationTime(grid) => grid.for_each(|column| {
                column.query(range, &mut cell_callback);
            }),
        }
        examined
    }

    #[allow(dead_code)]
    fn query_duration<F: FnMut(&Interval)>(&self, duration: DurationRange, mut action: F) -> u32 {
        let mut examined = 0;
        let mut cell_callback = |cell: &Vec<Interval>| {
            // traverse the cell by decreasing end time
            for interval in cell.iter().rev() {
                examined += 1;
                if duration.contains(interval) {
                    action(interval);
                }
            }
        };
        match self {
            Self::TimeDuration(grid) => {
                grid.for_each(|column| {
                    column.query(duration, &mut cell_callback);
                });
            }
            Self::DurationTime(grid) => grid.query(duration, |column| {
                column.for_each(&mut cell_callback);
            }),
        }
        examined
    }
}

#[derive(DeepSizeOf)]
struct TimePartition<V> {
    max_start_times: Vec<Time>,
    max_end_times: Vec<Time>,
    values: Vec<V>,
}

impl<V> TimePartition<V> {
    #[allow(dead_code)]
    fn new<B: FnMut(&mut [Interval]) -> V>(
        intervals: &mut [Interval],
        block: usize,
        mut inner_builder: B,
    ) -> Self {
        let mut max_start_times = Vec::new();
        let mut max_end_times = Vec::new();
        let mut values = Vec::new();

        intervals.sort_unstable_by_key(|interval| interval.start);

        let mut start = 0usize;

        while start < intervals.len() {
            let end = next_breakpoint(&intervals, start, block, |interval| interval.start);
            max_start_times.push(intervals[end].start);
            max_end_times.push(
                intervals[start..=end]
                    .iter()
                    .map(|interval| interval.end)
                    .max()
                    .unwrap(),
            );
            values.push(inner_builder(&mut intervals[start..=end]));
            start = end + 1;
        }

        Self {
            max_start_times,
            max_end_times,
            values,
        }
    }

    #[allow(dead_code)]
    fn query<F: FnMut(&V)>(&self, range: Interval, mut action: F) {
        let end = match self.max_start_times.binary_search(&range.end) {
            Ok(i) => i,  // exact match, equal end time than maximum start time
            Err(i) => i, // maximum start time strictly less than end time
        };

        for i in (0..=end).rev() {
            if range.start >= self.max_end_times[i] {
                return;
            }
            action(&self.values[i]);
        }
    }

    fn for_each<F: FnMut(&V)>(&self, mut action: F) {
        for v in &self.values {
            action(v);
        }
    }
}

#[derive(DeepSizeOf)]
struct DurationPartition<V> {
    max_durations: Vec<Time>,
    min_durations: Vec<Time>,
    values: Vec<V>,
}

impl<V> DurationPartition<V> {
    #[allow(dead_code)]
    fn new<B: FnMut(&mut [Interval]) -> V>(
        intervals: &mut [Interval],
        block: usize,
        mut inner_builder: B,
    ) -> Self {
        let mut max_durations = Vec::new();
        let mut min_durations = Vec::new();
        let mut values = Vec::new();

        intervals.sort_unstable_by_key(|interval| interval.duration());

        let mut start = 0usize;

        while start < intervals.len() {
            let end = next_breakpoint(&intervals, start, block, |interval| interval.duration());
            max_durations.push(intervals[end].duration());
            min_durations.push(intervals[start].duration());
            values.push(inner_builder(&mut intervals[start..=end]));
            start = end + 1;
        }

        Self {
            max_durations,
            min_durations,
            values,
        }
    }

    #[allow(dead_code)]
    fn query<F: FnMut(&V)>(&self, duration_range: DurationRange, mut action: F) {
        let end = match self.min_durations.binary_search(&duration_range.min) {
            Ok(i) => i,
            Err(i) => i,
        };

        for i in (0..=end).rev() {
            if duration_range.min > self.max_durations[i] {
                return;
            }
            action(&self.values[i]);
        }
    }

    fn for_each<F: FnMut(&V)>(&self, mut action: F) {
        for v in &self.values {
            action(v);
        }
    }
}
