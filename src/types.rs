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

pub trait Algorithm {
    fn name(&self) -> String;
    fn parameters(&self) -> String;
    fn version(&self) -> u8;
    fn index(&mut self, dataset: &[Interval]);
    fn run(&self, queries: &[Query]) -> Vec<Vec<usize>>;
}
