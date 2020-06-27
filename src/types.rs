pub type Time = u32;

#[derive(Debug, Clone)]
pub struct Interval {
    start: Time,
    end: Time,
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
        self.start < other.end || self.end > other.start
    }
}

#[derive(Debug, Clone)]
pub struct DurationRange {
    min: Time,
    max: Time,
}

impl DurationRange {
    pub fn new(min: Time, max: Time) -> Self {
        Self { min, max }
    }

    pub fn contains(&self, interval: &Interval) -> bool {
        let d = interval.duration();
        self.min <= d && d <= self.max
    }
}

#[derive(Debug, Clone)]
pub struct Query {
    pub range: Option<Interval>,
    pub duration: Option<DurationRange>,
}

#[derive(Debug)]
pub struct QueryAnswer {
    elapsed: std::time::Duration,
    indices: Vec<usize>
}

impl QueryAnswer {
    pub fn elapsed_millis(&self) -> i64 {
        self.elapsed.as_millis() as i64
    }

    pub fn num_matches(&self) -> u32 {
        self.indices.len() as u32
    }

    pub fn builder(expected: usize) -> QueryAnswerBuilder {
        QueryAnswerBuilder {
            start: std::time::Instant::now(),
            indices: Vec::with_capacity(expected),
        }
    }
}

pub struct QueryAnswerBuilder {
    start: std::time::Instant,
    indices: Vec<usize>,
}

impl QueryAnswerBuilder {
    pub fn push(&mut self, index: usize) {
        self.indices.push(index);
    }

    pub fn finalize(self) -> QueryAnswer {
        QueryAnswer {
            elapsed: std::time::Instant::now() - self.start,
            indices: self.indices
        }
    }
}

pub trait Algorithm {
    fn name(&self) -> String;
    fn parameters(&self) -> String;
    fn version(&self) -> u8;
    fn index(&mut self, dataset: &[Interval]);
    fn run(&self, queries: &[Query]) -> Vec<QueryAnswer>;
}
