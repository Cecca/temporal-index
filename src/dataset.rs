extern crate rand;
extern crate rand_xoshiro;
//extern crate zipf;

use crate::types::*;
use crate::zipf::ZipfDistribution;
use csv;
use rand::distributions::*;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;
use std::path::PathBuf;
use std::rc::Rc;

pub trait Dataset: std::fmt::Debug {
    fn name(&self) -> String;
    fn parameters(&self) -> String;
    fn get(&self) -> Vec<Interval>;
    fn version(&self) -> u8;

    fn descr(&self) -> String {
        format!(
            "{} ({}) [v{}]",
            self.name(),
            self.parameters(),
            self.version()
        )
    }
}

#[derive(Debug, Deserialize, Serialize, Copy, Clone)]
pub enum TimeDistribution {
    Uniform { low: Time, high: Time },
    Zipf { n: usize, beta: f64 },
    Clustered { n: usize, high: Time, std_dev: Time },
}

impl TimeDistribution {
    #[allow(deprecated)]
    fn stream<R: rand::Rng + Clone + 'static>(
        &self,
        rng: R,
    ) -> Box<dyn Iterator<Item = Time> + '_> {
        match &self {
            Self::Uniform { low, high } => {
                let d = Uniform::new(low, high);
                Box::new(d.sample_iter(rng))
            }
            Self::Zipf { n, beta } => {
                let d =
                    ZipfDistribution::new(*n, *beta).expect("problem creating zipf distribution");
                Box::new(d.sample_iter(rng))
            }
            Self::Clustered { n, high, std_dev } => {
                use std::iter::FromIterator;
                let mut rng = rng.clone();
                let centers_d = Uniform::new(0, high);
                let distribs: Vec<Normal> = Vec::from_iter(
                    centers_d
                        .sample_iter(&mut rng)
                        .take(*n)
                        .map(|center| Normal::new(center as f64, *std_dev as f64)),
                );

                Box::new(distribs.into_iter().cycle().flat_map(move |d| {
                    let s = rng.sample(d);
                    if s > 0.0 && s < std::u64::MAX as f64 {
                        Some(s as Time)
                    } else {
                        None
                    }
                    // assert!(s < std::u64::MAX as f64);
                    // s as Time
                }))
            }
        }
    }

    pub fn name(&self) -> String {
        match &self {
            Self::Uniform { .. } => String::from("uniform"),
            Self::Zipf { .. } => String::from("zipf"),
            Self::Clustered { .. } => String::from("clustered"),
        }
    }

    pub fn parameters(&self, prefix: &str) -> String {
        match &self {
            Self::Uniform { low, high } => format!("{}low={} {}high={}", prefix, low, prefix, high),
            Self::Zipf { n, beta } => format!("{}n={} {}beta={}", prefix, n, prefix, beta),
            Self::Clustered { n, high, std_dev } => format!(
                "{}n={} {}high={} {}stddev={}",
                prefix, n, prefix, high, prefix, std_dev
            ),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RandomDataset {
    seed: u64,
    n: usize,
    start_times: TimeDistribution,
    durations: TimeDistribution,
}

impl RandomDataset {
    pub fn new(
        seed: u64,
        n: usize,
        start_times: TimeDistribution,
        durations: TimeDistribution,
    ) -> Self {
        Self {
            seed,
            n,
            start_times,
            durations,
        }
    }
}

impl Dataset for RandomDataset {
    fn name(&self) -> String {
        format!(
            "random-{}-{}",
            self.start_times.name(),
            self.durations.name()
        )
    }

    fn parameters(&self) -> String {
        format!(
            "seed={} n={} {} {}",
            self.seed,
            self.n,
            self.start_times.parameters("start_"),
            self.durations.parameters("dur_")
        )
    }

    fn version(&self) -> u8 {
        6
    }

    /// Does not remove the duplicates, because otherwise the distributions
    /// would not reflect the desired ones. For instance, if we want a ZIPF distribution
    /// on the durations, but start times are in the range 1..1000, then there are only
    /// 999 possible intervals of length 1, which make it impossible to have a true ZIPF
    /// distribution on durations
    fn get(&self) -> Vec<Interval> {
        use rand::RngCore;
        let mut seeder = rand_xoshiro::SplitMix64::seed_from_u64(self.seed);
        let rng1 = Xoshiro256PlusPlus::seed_from_u64(seeder.next_u64());
        let rng2 = Xoshiro256PlusPlus::seed_from_u64(seeder.next_u64());
        let mut data = Vec::new();
        let mut start_times = self.start_times.stream(rng1);
        let mut durations = self.durations.stream(rng2);
        for _ in 0..self.n {
            let interval = Interval::new(start_times.next().unwrap(), durations.next().unwrap());
            data.push(interval);
        }
        data
    }
}

#[derive(Debug, Clone)]
pub struct RandomDatasetZipfAndUniform {
    seed: u64,
    n: usize,
    exponent: f64,
    max_time: Time,
}

impl RandomDatasetZipfAndUniform {
    pub fn new(seed: u64, n: usize, exponent: f64, max_time: Time) -> Self {
        Self {
            seed,
            n,
            exponent,
            max_time,
        }
    }
}

impl Dataset for RandomDatasetZipfAndUniform {
    fn name(&self) -> String {
        String::from("zipf-and-uniform")
    }

    fn parameters(&self) -> String {
        format!(
            "{},{},{},{}",
            self.seed, self.n, self.exponent, self.max_time
        )
    }

    fn version(&self) -> u8 {
        2
    }

    fn get(&self) -> Vec<Interval> {
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(self.seed);
        let duration_distribution = ZipfDistribution::new(self.n, self.exponent)
            .expect("problem creating Zipf distribution");
        let start_time_distribution = Uniform::new(0, self.max_time);
        let mut data = Vec::new();
        for _ in 0..self.n {
            let interval = Interval::new(
                start_time_distribution.sample(&mut rng),
                duration_distribution.sample(&mut rng),
            );
            data.push(interval)
        }
        data.sort();
        data.dedup();
        data
    }
}

pub trait Queryset: std::fmt::Debug {
    fn name(&self) -> String;
    fn parameters(&self) -> String;
    fn version(&self) -> u8;
    fn get(&self) -> Vec<Query>;

    fn descr(&self) -> String {
        format!(
            "{} ({}) [v{}]",
            self.name(),
            self.parameters(),
            self.version()
        )
    }
}

#[derive(Debug)]
pub struct RandomQueriesZipfAndUniform {
    seed: u64,
    n: usize,
    exponent: f64,
    max_start_time: Time,
    max_duration_factor: f64,
}

impl RandomQueriesZipfAndUniform {
    pub fn new(
        seed: u64,
        n: usize,
        exponent: f64,
        max_start_time: Time,
        max_duration_factor: f64,
    ) -> Self {
        Self {
            seed,
            n,
            exponent,
            max_start_time,
            max_duration_factor,
        }
    }
}

impl Queryset for RandomQueriesZipfAndUniform {
    fn name(&self) -> String {
        String::from("zipf-and-uniform")
    }

    fn parameters(&self) -> String {
        format!(
            "{}:{}:{}:{}:{}",
            self.seed, self.n, self.exponent, self.max_start_time, self.max_duration_factor
        )
    }

    fn version(&self) -> u8 {
        2
    }

    fn get(&self) -> Vec<Query> {
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(self.seed);
        let interval_duration_distribution = ZipfDistribution::new(self.n, self.exponent)
            .expect("problem creating Zipf distribution");
        let start_time_distribution = Uniform::new(0, self.max_start_time);
        let duration_factor_distribution = Uniform::new(0.01, self.max_duration_factor);
        let mut data = Vec::new();
        for _ in 0..self.n {
            let range = Interval::new(
                start_time_distribution.sample(&mut rng),
                interval_duration_distribution.sample(&mut rng),
            );
            let (d_a, d_b) = (
                duration_factor_distribution.sample(&mut rng),
                duration_factor_distribution.sample(&mut rng),
            );
            let (d_min, d_max) = if d_a < d_b { (d_a, d_b) } else { (d_b, d_a) };
            let d_min = std::cmp::max((d_min * range.duration() as f64) as Time, 1);
            let d_max = std::cmp::max((d_max * range.duration() as f64) as Time, 1);
            data.push(Query {
                range: Some(range),
                duration: Some(DurationRange::new(d_min as Time, d_max as Time)),
            });
        }
        data.sort();
        data.dedup();
        data
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RandomQueryset {
    seed: u64,
    n: usize,
    intervals: Option<(TimeDistribution, TimeDistribution)>,
    durations: Option<(TimeDistribution, TimeDistribution)>,
}

impl RandomQueryset {
    pub fn new(
        seed: u64,
        n: usize,
        intervals: Option<(TimeDistribution, TimeDistribution)>,
        durations: Option<(TimeDistribution, TimeDistribution)>,
    ) -> Self {
        Self {
            seed,
            n,
            intervals,
            durations,
        }
    }
}

impl Queryset for RandomQueryset {
    fn name(&self) -> String {
        format!(
            "random-{}-{}",
            self.intervals
                .map(|pair| format!("{}-{}", pair.0.name(), pair.1.name()))
                .unwrap_or("None".to_owned()),
            self.durations
                .map(|pair| format!("{}-{}", pair.0.name(), pair.1.name()))
                .unwrap_or("None".to_owned()),
        )
    }

    fn parameters(&self) -> String {
        format!(
            "seed={} n={} {} {}",
            self.seed,
            self.n,
            self.intervals
                .map(|it| format!("{} {}", it.0.parameters("start_"), it.1.parameters("dur_")))
                .unwrap_or(String::new()),
            self.durations
                .map(|d| format!(
                    "{} {}",
                    d.0.parameters("durmin_"),
                    d.1.parameters("durmax_")
                ))
                .unwrap_or(String::new()),
        )
    }

    fn version(&self) -> u8 {
        6
    }

    fn get(&self) -> Vec<Query> {
        use rand::RngCore;
        use std::collections::BTreeSet;
        let mut seeder = rand_xoshiro::SplitMix64::seed_from_u64(self.seed);
        let rng1 = Xoshiro256PlusPlus::seed_from_u64(seeder.next_u64());
        let rng2 = Xoshiro256PlusPlus::seed_from_u64(seeder.next_u64());
        let rng3 = Xoshiro256PlusPlus::seed_from_u64(seeder.next_u64());
        let rng4 = Xoshiro256PlusPlus::seed_from_u64(seeder.next_u64());
        let mut data = BTreeSet::new();
        let mut interval_gen = self
            .intervals
            .as_ref()
            .map(move |(start_times, durations)| {
                (start_times.stream(rng1), durations.stream(rng2))
            });
        let mut durations_gen = self
            .durations
            .as_ref()
            .map(move |(start, duration)| (start.stream(rng3), duration.stream(rng4)));
        while data.len() < self.n {
            let interval = interval_gen.as_mut().map(|(start, duration)| {
                Interval::new(start.next().unwrap(), duration.next().unwrap())
            });
            let duration_range = durations_gen.as_mut().map(|(start, duration)| {
                let duration_range_start = start.next().unwrap();
                let duration_range_duration = duration.next().unwrap();
                DurationRange::new(
                    duration_range_start,
                    duration_range_start + duration_range_duration,
                )
            });
            data.insert(Query {
                range: interval,
                duration: duration_range,
            });
        }
        data.into_iter().collect()
    }
}

#[derive(Debug)]
pub struct MixedQueryset {
    inner: Vec<Rc<dyn Queryset>>,
}

impl MixedQueryset {
    pub fn new(sets: Vec<Rc<dyn Queryset>>) -> Self {
        Self { inner: sets }
    }
}

impl Queryset for MixedQueryset {
    fn name(&self) -> String {
        "Mixed".to_owned()
    }

    fn parameters(&self) -> String {
        let ps: Vec<String> = self.inner.iter().map(|x| x.parameters()).collect();
        ps.join("|")
    }

    fn version(&self) -> u8 {
        1
    }

    fn get(&self) -> Vec<Query> {
        self.inner.iter().flat_map(|inner| inner.get()).collect()
    }
}

#[derive(Debug)]
pub struct CsvDataset {
    start_column: usize,
    end_column: usize,
    path: PathBuf,
    separator: u8,
    has_header: bool,
}

impl CsvDataset {
    pub fn new(
        path: PathBuf,
        start_column: usize,
        end_column: usize,
        separator: u8,
        has_header: bool,
    ) -> Self {
        Self {
            start_column,
            end_column,
            path,
            separator,
            has_header,
        }
    }
}

impl Dataset for CsvDataset {
    fn name(&self) -> String {
        format!("csv({:?})", self.path)
    }

    fn parameters(&self) -> String {
        format!(
            "start_column={} end_column={}",
            self.start_column, self.end_column
        )
    }

    fn get(&self) -> Vec<Interval> {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(self.has_header)
            .delimiter(self.separator)
            .from_path(&self.path)
            .expect("cannot get data file");

        let pairs: Vec<(Time, Time)> = reader
            .records()
            .map(|record| {
                let record = record.expect("problems decoding record");
                let start = record
                    .get(self.start_column)
                    .unwrap()
                    .parse::<u64>()
                    .expect("problem parsing start time");
                let end = record
                    .get(self.end_column)
                    .unwrap()
                    .parse::<u64>()
                    .expect("problem parsing end time");
                assert!(end >= start, "start={} end={}", start, end);
                (start, end + 1)
            })
            .collect();

        let min_time = pairs.iter().map(|p| p.0).min().unwrap();
        pairs
            .iter()
            .map(|(s, e)| Interval {
                start: *s - min_time,
                end: *e - min_time,
            })
            .collect()
    }

    fn version(&self) -> u8 {
        1
    }
}
