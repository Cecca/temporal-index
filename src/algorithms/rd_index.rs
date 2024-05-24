use crate::types::*;
use rayon::prelude::*;
use std::sync::atomic::AtomicU32;

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub enum DimensionOrder {
    TimeDuration,
    DurationTime,
}

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

    pub fn export_example() -> anyhow::Result<()> {
        use crate::dataset::Dataset;
        use chrono::prelude::*;
        use std::fs::File;
        use std::io::prelude::*;

        let page_size = 70;

        let full_dataset = crate::dataset::MimicIIIDataset::from_upstream()
            .unwrap()
            .get()
            .unwrap();

        let outdir = std::path::PathBuf::from("example_rdindex");
        if !outdir.is_dir() {
            std::fs::create_dir(&outdir)?;
        }

        let basedate = Utc.ymd(2016, 01, 01);
        let filter_start = Utc.ymd(2016, 01, 01);
        let filter_end = Utc.ymd(2016, 12, 31);

        let dataset: Vec<Interval> = full_dataset
            .into_iter()
            .filter(|interval| {
                let d = basedate + chrono::Duration::days(interval.start as i64);
                filter_start <= d && d <= filter_end
            })
            .filter(|interval| interval.duration() <= 50)
            .collect();

        // let mut rng = Xoshiro256PlusPlus::seed_from_u64(23684);
        // let dataset: Vec<Interval> = dataset.choose_multiple(&mut rng, 10000).cloned().collect();

        let mut index = RDIndex::new(DimensionOrder::TimeDuration, page_size);
        index.index(dataset.as_slice());
        match index.grid.unwrap() {
            Grid::DurationTime(_) => panic!(),
            Grid::TimeDuration(columns) => {
                let mut cinfo = File::create(outdir.join("column_info.csv"))?;
                writeln!(cinfo, "i,column_bound,latest_end_time")?;
                for (i, (bound, max_end)) in columns
                    .min_start_times
                    .iter()
                    .zip(columns.max_end_times.iter())
                    .enumerate()
                {
                    let bound = basedate + chrono::Duration::days(*bound as i64);
                    let max_end = basedate + chrono::Duration::days(*max_end as i64);
                    writeln!(
                        cinfo,
                        "{},{},{}",
                        i,
                        bound.format("%Y-%m-%d"),
                        max_end.format("%Y-%m-%d")
                    )?;
                }
                drop(cinfo);
                let mut cell_info = File::create(outdir.join("cell_info.csv"))?;
                writeln!(cell_info, "i,j,cell_bound,max_duration,cell_size,heavy")?;
                for (i, cells) in columns.values.iter().enumerate() {
                    for (j, ((min_duration, max_duration), cell)) in cells
                        .min_durations
                        .iter()
                        .zip(cells.max_durations.iter())
                        .zip(cells.values.iter())
                        .enumerate()
                    {
                        writeln!(
                            cell_info,
                            "{},{},{},{},{},{}",
                            i,
                            j,
                            min_duration,
                            max_duration,
                            cell.len(),
                            cell.len() > page_size
                        )?;
                    }
                }
                drop(cell_info);
                let mut data_out = File::create(outdir.join("example_dataset.csv"))?;
                writeln!(data_out, "start,duration")?;
                for interval in dataset {
                    let start = basedate + chrono::Duration::days(interval.start as i64);
                    writeln!(
                        data_out,
                        "{},{}",
                        start.format("%Y-%m-%d"),
                        interval.duration()
                    )?;
                }
            }
        }

        Ok(())
    }
}

impl Algorithm for RDIndex {
    fn name(&self) -> String {
        "rd-index".to_owned()
    }

    fn size(&self) -> usize {
        if let Some(grid) = self.grid.as_ref() {
            grid.len()
        } else {
            0
        }
    }

    fn parameters(&self) -> String {
        format!(
            "dimension_order={:?} page_size={}",
            self.dimension_order, self.page_size
        )
    }

    fn version(&self) -> u8 {
        2
    }

    fn index(&mut self, dataset: &[Interval]) {
        use std::iter::FromIterator;
        let mut dataset = Vec::from_iter(dataset.iter().cloned());
        self.grid.replace(Grid::new(
            &mut dataset,
            self.page_size,
            self.dimension_order,
        ));
        assert!(
            self.grid.as_ref().unwrap().len() == dataset.len(),
            "not all the intervals are indexed! indexed: {}, dataset: {}",
            self.grid.as_ref().unwrap().len(),
            dataset.len()
        );
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
    }

    fn par_query(&self, query: &Query, answer: &mut QueryAnswerBuilder) {
        let (sender, receiver) = crossbeam_channel::unbounded();
        if let Some(grid) = &self.grid {
            let examined = match (query.range, query.duration) {
                (Some(range), Some(duration)) => {
                    grid.par_query_range_duration(range, duration, |interval| {
                        sender.send(*interval).unwrap()
                    })
                }
                (Some(_range), None) => unimplemented!(),
                (None, Some(_duration)) => unimplemented!(),
                (None, None) => unimplemented!("iteration is not supported"),
            };
            drop(sender);
            for interval in receiver {
                answer.push(interval);
            }
            answer.inc_examined(examined);
        } else {
            panic!("uninitialized index!");
        }
    }

    fn clear(&mut self) {
        drop(self.grid.take());
    }

    fn insert(&mut self, x: Interval) {
        if let Some(grid) = self.grid.as_mut() {
            grid.insert(x, self.page_size);
        } else {
            self.grid
                .replace(Grid::new(&mut [x], self.page_size, self.dimension_order));
        }
    }

    fn remove(&mut self, x: Interval) {
        if let Some(grid) = self.grid.as_mut() {
            grid.remove(x, self.page_size);
        }
    }
}

fn next_breakpoint<F: Fn(&Interval) -> Time>(
    intervals: &[Interval],
    start: usize,
    block: usize,
    key: F,
) -> usize {
    if start + block >= intervals.len() - 1 {
        return intervals.len() - 1;
    }

    let lookahead = start + block;
    let lookahead_key = key(&intervals[lookahead]);
    let mut end = lookahead;
    if key(&intervals[start]) == lookahead_key {
        // We are looking at a heavy column/cell
        while end < intervals.len() - 1 && lookahead_key == key(&intervals[end + 1]) {
            end += 1;
        }
    } else {
        while end > start && key(&intervals[end]) == key(&intervals[end + 1]) {
            end -= 1;
        }
    }
    end
}
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

    /// return the number of indexed intervals
    fn len(&self) -> usize {
        let mut cnt = 0;
        match self {
            Self::TimeDuration(grid) => {
                grid.for_each(|column| {
                    column.for_each(|cell| cnt += cell.len());
                });
            }
            Self::DurationTime(grid) => grid.for_each(|column| {
                column.for_each(|cell| cnt += cell.len());
            }),
        }
        cnt
    }

    #[allow(dead_code)]
    fn par_query_range_duration<F: Fn(&Interval) + Send + Sync>(
        &self,
        range: Interval,
        duration: DurationRange,
        action: F,
    ) -> u32 {
        let examined = AtomicU32::new(0);
        let cell_callback = |cell: &Vec<Interval>| {
            // traverse the cell by decreasing end time
            let mut cnt = 0u32;
            for interval in cell.iter().rev() {
                cnt += 1;
                if interval.end <= range.start {
                    return;
                }
                if duration.contains(interval) && interval.start < range.end {
                    action(interval);
                }
            }
            examined.fetch_add(cnt, std::sync::atomic::Ordering::SeqCst);
        };
        match self {
            Self::TimeDuration(grid) => {
                grid.par_query(range, |column| {
                    column.par_query(duration, cell_callback);
                });
            }
            Self::DurationTime(grid) => grid.par_query(duration, |column| {
                column.par_query(range, cell_callback);
            }),
        }
        examined.load(std::sync::atomic::Ordering::SeqCst)
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

    pub fn insert(&mut self, interval: Interval, page_size: usize) {
        let prev_size = self.len();
        match self {
            Grid::TimeDuration(grid) => {
                assert!(!grid.values.is_empty());

                // First find the column that could hold the element
                let i = find_pos(&grid.min_start_times, |t| t <= interval.start);

                let column_size = grid.values[i].size;
                if column_size + 1 > page_size * page_size && !grid.is_heavy(i) {
                    let mut intervals = grid.values[i].all_intervals();
                    intervals.push(interval);
                    grid.replace(i, time_columns(&mut intervals, page_size / 2));
                } else {
                    // Find the right cell and insert, possibly splitting
                    let column = &mut grid.values[i];
                    let j = find_pos(&column.min_durations, |d| d <= interval.duration());

                    let cell_size = column.values[j].len();
                    if cell_size + 1 > page_size && !column.is_heavy(j) {
                        let mut new_cells = column.values[j].all_intervals();
                        new_cells.push(interval);
                        column.replace(j, duration_cells(&mut new_cells, page_size / 2));
                    } else {
                        insert_by_end(&mut column.values[j], interval);
                    }

                    column.size += 1;
                }

                grid.size += 1;
                debug_assert!(grid.size == grid.all_intervals().len());
            }
            Grid::DurationTime(grid) => {
                assert!(!grid.values.is_empty());

                // First find the column that could hold the element
                let i = find_pos(&grid.min_durations, |d| d <= interval.duration());

                let column_size = grid.values[i].size;
                if column_size + 1 > page_size * page_size && !grid.is_heavy(i) {
                    // println!("Replacing column");
                    let mut intervals = grid.values[i].all_intervals();
                    intervals.push(interval);
                    grid.replace(i, duration_columns(&mut intervals, page_size / 2));
                } else {
                    // Find the right cell and insert, possibly splitting
                    let column = &mut grid.values[i];
                    let j = find_pos(&column.min_start_times, |d| d <= interval.start);

                    let cell_size = column.values[j].len();
                    if cell_size + 1 > page_size && !column.is_heavy(j) {
                        // println!("replacing cell");
                        let mut new_cells = column.values[j].all_intervals();
                        new_cells.push(interval);
                        column.replace(j, time_cells(&mut new_cells, page_size / 2));
                    } else {
                        // println!("Insert in place");
                        insert_by_end(&mut column.values[j], interval);
                    }

                    column.size += 1;
                }

                grid.size += 1;
                debug_assert!(grid.size == grid.all_intervals().len());
            }
        }
        debug_assert_eq!(self.len(), prev_size + 1);
    }

    pub fn remove(&mut self, interval: Interval, page_size: usize) {
        match self {
            Grid::TimeDuration(grid) => {
                if grid.values.is_empty() {
                    return;
                }

                // First find the column that could be holding the element
                let i = find_pos(&grid.min_start_times, |t| t <= interval.start);
                let j = find_pos(&grid.values[i].min_durations, |d| d <= interval.duration());

                // Remove the element with a sequential scan
                let prev_size = grid.values[i].values[j].len();
                grid.values[i].values[j].retain(|int| int != &interval);
                let new_size = grid.values[i].values[j].len();
                if prev_size == new_size {
                    // no removal happened, do nothing more
                    return;
                }

                // decrease the size
                grid.values[i].size -= 1;
                let column_size = grid.values[i].size;

                // Restructure the grid, possibly
                if column_size < page_size * page_size {
                    grid.maybe_merge(i, page_size, page_size * page_size);
                } else if grid.values[i].values[j].len() < page_size {
                    grid.values[i].maybe_merge(j, page_size, page_size);
                }

                grid.size -= 1;
                debug_assert!(grid.size == grid.all_intervals().len());
            }
            Grid::DurationTime(grid) => {
                if grid.values.is_empty() {
                    return;
                }

                // First find the column that could be holding the element
                let i = find_pos(&grid.min_durations, |t| t <= interval.duration());
                let j = find_pos(&grid.values[i].min_start_times, |d| d <= interval.start);

                // Remove the element with a sequential scan
                let prev_size = grid.values[i].values[j].len();
                grid.values[i].values[j].retain(|int| int != &interval);
                let new_size = grid.values[i].values[j].len();
                if prev_size == new_size {
                    // no removal happened, do nothing more
                    return;
                }

                // decrease the size
                grid.values[i].size -= 1;
                let column_size = grid.values[i].size;

                // Restructure the grid, possibly
                if column_size < page_size * page_size {
                    grid.maybe_merge(i, page_size, page_size * page_size);
                } else if grid.values[i].values[j].len() < page_size {
                    grid.values[i].maybe_merge(j, page_size, page_size);
                }

                grid.size -= 1;
                debug_assert!(grid.size == grid.all_intervals().len());
            }
        }
    }
}

fn cell_builder(cell: &mut [Interval]) -> Vec<Interval> {
    use std::iter::FromIterator;
    cell.sort_unstable_by_key(|interval| interval.end);
    Vec::from_iter(cell.iter().cloned())
}

/// Find the first position satisfying the given predicate on time, assuming that the times are sorted.
fn find_pos(v: &[Time], pred: impl Fn(Time) -> bool) -> usize {
    let mut i = v.len() - 1;
    while i > 0 {
        if pred(v[i]) {
            return i;
        }
        i -= 1;
    }
    0
}

fn insert_by_end(v: &mut Vec<Interval>, interval: Interval) {
    // println!("before insert {:?}", v);
    let iend = interval.end;
    if iend >= v[v.len() - 1].end {
        // handle the simple and efficient case
        v.push(interval);
    } else {
        let mut pos = 0;
        while pos < v.len() {
            if iend < v[pos].end {
                break;
            }
            pos += 1;
        }
        v.insert(pos, interval);
    }
    debug_assert!(is_sorted_by(&v, |a, b| a.end <= b.end), "cell is {:?}", v);
}

fn time_columns(
    intervals: &mut [Interval],
    page_size: usize,
) -> TimePartition<DurationPartition<Vec<Interval>>> {
    TimePartition::new(intervals, page_size * page_size, |column| {
        DurationPartition::new(column, page_size, cell_builder)
    })
}

fn time_cells(intervals: &mut [Interval], page_size: usize) -> TimePartition<Vec<Interval>> {
    TimePartition::new(intervals, page_size, cell_builder)
}

fn duration_columns(
    intervals: &mut [Interval],
    page_size: usize,
) -> DurationPartition<TimePartition<Vec<Interval>>> {
    DurationPartition::new(intervals, page_size * page_size, |column| {
        TimePartition::new(column, page_size, cell_builder)
    })
}

fn duration_cells(
    intervals: &mut [Interval],
    page_size: usize,
) -> DurationPartition<Vec<Interval>> {
    DurationPartition::new(intervals, page_size, cell_builder)
}

struct TimePartition<V> {
    min_start_times: Vec<Time>,
    max_end_times: Vec<Time>,
    values: Vec<V>,
    size: usize,
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
            max_start_times.push(intervals[start].start);
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

        // do the cumulative maximum end time.
        // we need to do this because early buckets might contain end times that are later
        // than the end times of the following buckets.
        let mut cum_max = 0;
        for t in max_end_times.iter_mut() {
            if *t > cum_max {
                cum_max = *t;
            }
            *t = cum_max;
        }

        Self {
            min_start_times: max_start_times,
            max_end_times,
            values,
            size: intervals.len(),
        }
    }

    fn replace(&mut self, i: usize, mut other: Self) {
        // println!(
        //     "[time] Replace {} out of {} with {} new",
        //     i,
        //     self.values.len(),
        //     other.values.len()
        // );
        // Remove the old column
        self.min_start_times.remove(i);
        self.max_end_times.remove(i);
        self.values.remove(i);

        // Add the new columns
        insert_many(i, &mut self.min_start_times, &mut other.min_start_times);
        insert_many(i, &mut self.max_end_times, &mut other.max_end_times);
        insert_many(i, &mut self.values, &mut other.values);
        debug_assert!(self.check_sorted());
    }

    fn check_sorted(&self) -> bool {
        is_sorted(&self.min_start_times)
    }

    #[allow(dead_code)]
    fn query<F: FnMut(&V)>(&self, range: Interval, mut action: F) {
        let end = match self.min_start_times.binary_search(&range.end) {
            Ok(i) => i, // exact match, equal end time than maximum start time
            Err(i) => std::cmp::min(i, self.min_start_times.len() - 1),
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

impl<V: Send + Sync> TimePartition<V> {
    fn par_query<F: Fn(&V) + Sync + Send>(&self, range: Interval, action: F) {
        let end = match self.min_start_times.binary_search(&range.end) {
            Ok(i) => i, // exact match, equal end time than maximum start time
            Err(i) => std::cmp::min(i, self.min_start_times.len() - 1),
        };

        let max_end_times = &self.max_end_times;

        (0..=end)
            .into_par_iter()
            .filter(|i| range.start < max_end_times[*i])
            .for_each(|i| {
                action(&self.values[i]);
            });
    }
}

struct DurationPartition<V> {
    max_durations: Vec<Time>,
    min_durations: Vec<Time>,
    values: Vec<V>,
    size: usize,
}

impl<V: Send + Sync> DurationPartition<V> {
    fn par_query<F: Fn(&V) + Send + Sync>(&self, duration_range: DurationRange, action: F) {
        let end = match self.min_durations.binary_search(&duration_range.max) {
            Ok(i) => i,
            Err(i) => std::cmp::min(i, self.min_durations.len() - 1),
        };

        (0..=end)
            .into_par_iter()
            .filter(|i| duration_range.min <= self.max_durations[*i])
            .for_each(|i| {
                action(&self.values[i]);
            });
    }
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
            size: intervals.len(),
        }
    }

    #[allow(dead_code)]
    fn query<F: FnMut(&V)>(&self, duration_range: DurationRange, mut action: F) {
        let end = match self.min_durations.binary_search(&duration_range.max) {
            Ok(i) => i,
            Err(i) => std::cmp::min(i, self.min_durations.len() - 1),
        };

        for i in (0..=end).rev() {
            if duration_range.min > self.max_durations[i] {
                return;
            }
            action(&self.values[i]);
        }
    }

    fn replace(&mut self, i: usize, mut other: Self) {
        // println!(
        //     "[duration] Replace {} out of {} with {} new {:?}",
        //     i,
        //     self.values.len(),
        //     other.values.len(),
        //     other.min_durations
        // );
        // println!("[duration] before remove {:?}", self.min_durations);
        // Remove the old column
        self.min_durations.remove(i);
        self.max_durations.remove(i);
        self.values.remove(i);
        // println!("[duration] after remove {:?}", self.min_durations);

        // Add the new columns
        insert_many(i, &mut self.min_durations, &mut other.min_durations);
        // println!("[duration] after insert many {:?}", self.min_durations);
        insert_many(i, &mut self.max_durations, &mut other.max_durations);
        insert_many(i, &mut self.values, &mut other.values);
        debug_assert!(self.check_sorted(), "not sorted {:?}", self.min_durations);
    }

    fn check_sorted(&self) -> bool {
        is_sorted(&self.min_durations) && is_sorted(&self.max_durations)
    }

    fn for_each<F: FnMut(&V)>(&self, mut action: F) {
        for v in &self.values {
            action(v);
        }
    }
}

fn insert_many<T>(i: usize, v: &mut Vec<T>, to_insert: &mut Vec<T>) {
    while let Some(new) = to_insert.pop() {
        v.insert(i, new);
    }
}

trait AllIntervals {
    fn all_intervals_i(&self, out: &mut Vec<Interval>);
    fn all_intervals(&self) -> Vec<Interval> {
        let mut out = Vec::new();
        self.all_intervals_i(&mut out);
        return out;
    }
}

impl AllIntervals for Vec<Interval> {
    fn all_intervals_i(&self, out: &mut Vec<Interval>) {
        out.extend(self);
    }
}

impl<V: AllIntervals> AllIntervals for TimePartition<V> {
    fn all_intervals_i(&self, out: &mut Vec<Interval>) {
        self.for_each(|v| {
            v.all_intervals_i(out);
        });
    }
}

impl<V: AllIntervals> AllIntervals for DurationPartition<V> {
    fn all_intervals_i(&self, out: &mut Vec<Interval>) {
        self.for_each(|v| {
            v.all_intervals_i(out);
        });
    }
}

trait NumIntervals {
    fn num_intervals(&self) -> usize;
}

impl NumIntervals for Vec<Interval> {
    fn num_intervals(&self) -> usize {
        self.len()
    }
}

impl<V> NumIntervals for DurationPartition<V> {
    fn num_intervals(&self) -> usize {
        self.size
    }
}

impl<V> NumIntervals for TimePartition<V> {
    fn num_intervals(&self) -> usize {
        self.size
    }
}

fn get_mergeable<N: NumIntervals>(i: usize, coll: &[N], max_size: usize) -> Option<usize> {
    let size_i = coll[i].num_intervals();
    if i >= 1 && coll[i - 1].num_intervals() + size_i <= max_size {
        Some(i - 1)
    } else if i + 1 < coll.len() && coll[i + 1].num_intervals() + size_i <= max_size {
        Some(i + 1)
    } else {
        None
    }
}

trait MaybeMerge {
    fn maybe_merge(&mut self, i: usize, page_size: usize, max_size: usize);
}

// merge cells partitioned by time
impl MaybeMerge for TimePartition<Vec<Interval>> {
    fn maybe_merge(&mut self, i: usize, _page_size: usize, max_size: usize) {
        if let Some(h) = get_mergeable(i, &self.values, max_size) {
            let intervals: Vec<Interval> = self.values[h].drain(..).collect();
            self.values[i].extend(intervals.into_iter());
            self.values[i].sort_unstable_by_key(|int| int.end);

            self.min_start_times[i] = self.values[i].iter().map(|int| int.start).min().unwrap();
            self.max_end_times[i] = self.values[i].iter().map(|int| int.end).max().unwrap();

            self.values.remove(h);
            self.min_start_times.remove(h);
            self.max_end_times.remove(h);
        }
    }
}

// merge cells partitioned by duration
impl MaybeMerge for DurationPartition<Vec<Interval>> {
    fn maybe_merge(&mut self, i: usize, _page_size: usize, max_size: usize) {
        if let Some(h) = get_mergeable(i, &self.values, max_size) {
            let intervals: Vec<Interval> = self.values[h].drain(..).collect();
            self.values[i].extend(intervals.into_iter());
            self.values[i].sort_unstable_by_key(|int| int.end);

            self.min_durations[i] = self.values[i]
                .iter()
                .map(|int| int.duration())
                .min()
                .unwrap();
            self.max_durations[i] = self.values[i]
                .iter()
                .map(|int| int.duration())
                .max()
                .unwrap();

            self.values.remove(h);
            self.min_durations.remove(h);
            self.max_durations.remove(h);
        }
    }
}

// merge columns partitioned by time
impl MaybeMerge for TimePartition<DurationPartition<Vec<Interval>>> {
    fn maybe_merge(&mut self, i: usize, page_size: usize, max_size: usize) {
        if let Some(h) = get_mergeable(i, &self.values, max_size) {
            let column_size = self.values[i].size;
            let mut intervals: Vec<Interval> =
                Vec::with_capacity(column_size + self.values[h].size);
            intervals.extend(self.values[i].all_intervals());
            intervals.extend(self.values[h].all_intervals());

            let mut new_column = duration_cells(&mut intervals, page_size);

            // swap out the old i column with the new one
            std::mem::swap(&mut self.values[i], &mut new_column);

            // remove the h column
            self.values.remove(h);
            self.min_start_times.remove(h);
            self.max_end_times.remove(h);
        }
    }
}

// merge columns partitioned by duration
impl MaybeMerge for DurationPartition<TimePartition<Vec<Interval>>> {
    fn maybe_merge(&mut self, i: usize, page_size: usize, max_size: usize) {
        if let Some(h) = get_mergeable(i, &self.values, max_size) {
            let column_size = self.values[i].size;
            let mut intervals: Vec<Interval> =
                Vec::with_capacity(column_size + self.values[h].size);
            intervals.extend(self.values[i].all_intervals());
            intervals.extend(self.values[h].all_intervals());

            let mut new_column = time_cells(&mut intervals, page_size);

            // swap out the old i column with the new one
            std::mem::swap(&mut self.values[i], &mut new_column);

            // remove the h column
            self.values.remove(h);
            self.min_durations.remove(h);
            self.max_durations.remove(h);
        }
    }
}

fn is_sorted(v: &[Time]) -> bool {
    for i in 1..v.len() {
        if v[i - 1] > v[i] {
            return false;
        }
    }
    return true;
}

fn is_sorted_by<T>(v: &[T], le: impl Fn(&T, &T) -> bool) -> bool {
    for i in 1..v.len() {
        if !le(&v[i - 1], &v[i]) {
            return false;
        }
    }
    return true;
}

trait IsHeavy {
    fn is_heavy(&self, i: usize) -> bool;
}

impl<V> IsHeavy for DurationPartition<V> {
    fn is_heavy(&self, i: usize) -> bool {
        self.min_durations[i] == self.max_durations[i]
    }
}

impl IsHeavy for TimePartition<Vec<Interval>> {
    fn is_heavy(&self, i: usize) -> bool {
        let max_start_time = self.values[i].iter().map(|int| int.start).max().unwrap();
        self.min_start_times[i] == max_start_time
    }
}

impl IsHeavy for TimePartition<DurationPartition<Vec<Interval>>> {
    fn is_heavy(&self, i: usize) -> bool {
        let mut max_start = 0;
        self.values[i].for_each(|ints| {
            for int in ints {
                if int.start > max_start {
                    max_start = int.start;
                }
            }
        });
        self.min_start_times[i] == max_start
    }
}
