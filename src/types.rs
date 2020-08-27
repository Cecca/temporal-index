use deepsize::DeepSizeOf;
use progress_logger::ProgressLogger;

pub type Time = u32;

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, DeepSizeOf)]
pub struct Interval {
    pub start: Time,
    pub end: Time,
}

impl Interval {
    pub fn new(start: Time, duration: Time) -> Self {
        Self {
            start,
            end: start + duration,
        }
    }

    pub fn duration(&self) -> Time {
        self.end - self.start
    }

    pub fn overlaps(&self, other: &Self) -> bool {
        self.start < other.end && other.start < self.end
        // !(self.start > other.end || other.start > self.end)
    }

    pub fn contains(&self, point: Time) -> bool {
        self.start <= point && point < self.end
    }

    pub fn middle(&self) -> Time {
        (self.end + self.start) / 2
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, DeepSizeOf)]
pub struct DurationRange {
    pub min: Time,
    pub max: Time,
}

impl DurationRange {
    pub fn new(min: Time, max: Time) -> Self {
        assert!(min <= max);
        Self { min, max }
    }

    pub fn contains(&self, interval: &Interval) -> bool {
        let d = interval.duration();
        self.min <= d && d <= self.max
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, DeepSizeOf)]
pub struct Query {
    pub range: Option<Interval>,
    pub duration: Option<DurationRange>,
}

#[derive(Debug)]
pub struct QueryAnswer {
    elapsed: std::time::Duration,
    intervals: Option<Vec<Interval>>,
    n_matches: u32,
}

impl QueryAnswer {
    pub fn elapsed_millis(&self) -> i64 {
        self.elapsed.as_millis() as i64
    }

    pub fn num_matches(&self) -> u32 {
        self.n_matches
    }

    pub fn builder() -> QueryAnswerBuilder {
        QueryAnswerBuilder {
            start: std::time::Instant::now(),
            intervals: None,
            n_matches: 0,
        }
    }

    #[cfg(test)]
    pub fn intervals(&self) -> Vec<Interval> {
        let mut res = self
            .intervals
            .as_ref()
            .expect("intervals not recorded")
            .clone();
        res.sort();
        res
    }
}

pub struct QueryAnswerBuilder {
    start: std::time::Instant,
    intervals: Option<Vec<Interval>>,
    n_matches: u32,
}

impl QueryAnswerBuilder {
    #[allow(dead_code)]
    pub fn record_intervals(self) -> Self {
        Self {
            start: std::time::Instant::now(),
            intervals: Some(Vec::new()),
            n_matches: 0,
        }
    }

    pub fn push(&mut self, interval: Interval) {
        if let Some(intervals) = self.intervals.as_mut() {
            intervals.push(interval);
        }
        self.n_matches += 1;
    }

    pub fn finalize(self) -> QueryAnswer {
        QueryAnswer {
            elapsed: std::time::Instant::now() - self.start,
            intervals: self.intervals,
            n_matches: self.n_matches,
        }
    }
}

pub trait Algorithm: std::fmt::Debug + DeepSizeOf {
    fn name(&self) -> String;
    fn parameters(&self) -> String;
    fn version(&self) -> u8;
    fn index(&mut self, dataset: &[Interval]);
    fn query(&self, query: &Query, answer: &mut QueryAnswerBuilder);
    /// Clears the index, freeing up space
    fn clear(&mut self);

    fn run(&self, queries: &[Query]) -> Vec<QueryAnswer> {
        let mut result = Vec::with_capacity(queries.len());
        let mut pl = ProgressLogger::builder()
            .with_expected_updates(queries.len() as u64)
            .with_items_name("queries")
            .start();
        for query in queries.iter() {
            let mut query_result = QueryAnswer::builder();
            self.query(query, &mut query_result);
            result.push(query_result.finalize());
            pl.update(1u64);
        }
        pl.stop();
        result
    }

    /// Run all the queries, and actually record the answered items
    fn run_recording(&self, queries: &[Query]) -> Vec<QueryAnswer> {
        let mut result = Vec::with_capacity(queries.len());
        let mut pl = ProgressLogger::builder()
            .with_expected_updates(queries.len() as u64)
            .with_items_name("queries")
            .start();
        for query in queries.iter() {
            let mut query_result = QueryAnswer::builder().record_intervals();
            self.query(query, &mut query_result);
            result.push(query_result.finalize());
            pl.update(1u64);
        }
        pl.stop();
        result
    }

    fn index_size(&self) -> usize {
        self.deep_size_of()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_overlap() {
        assert!(Interval::new(0, 3).overlaps(&Interval::new(2, 3)));
        assert!(!Interval::new(0, 3).overlaps(&Interval::new(4, 3)));
        assert!(Interval::new(0, 6).overlaps(&Interval::new(4, 3)));
        assert!(Interval::new(0, 6).overlaps(&Interval::new(2, 3)));
        assert!(!Interval::new(17, 1).overlaps(&Interval::new(18, 22)))
    }
}
