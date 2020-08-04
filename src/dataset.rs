extern crate rand;
extern crate rand_xoshiro;
//extern crate zipf;

use crate::types::*;
use crate::zipf::ZipfDistribution;
use rand::distributions::*;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;

pub trait Dataset: std::fmt::Debug {
    fn name(&self) -> String;
    fn parameters(&self) -> String;
    fn get(&self) -> Vec<Interval>;
    fn version(&self) -> u8;

}

#[derive(Debug, Deserialize, Serialize, Copy, Clone)]
pub enum TimeDistribution {
    Uniform { low: u32, high: u32 },
    Zipf { n: usize, beta: f64 },
}

impl TimeDistribution {
    fn stream<R: rand::Rng + 'static>(&self, rng: R) -> Box<dyn Iterator<Item = Time> + '_> {
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
        }
    }

    pub fn name(&self) -> String {
        match &self {
            Self::Uniform { .. } => String::from("uniform"),
            Self::Zipf { .. } => String::from("zipf"),
        }
    }

    pub fn parameters(&self) -> String {
        match &self {
            Self::Uniform { low, high } => format!("{},{}", low, high),
            Self::Zipf { n, beta } => format!("{},{}", n, beta),
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
            "{},{}_{}_{}",
            self.seed,
            self.n,
            self.start_times.parameters(),
            self.durations.parameters()
        )
    }

    fn version(&self) -> u8 {
        2
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
        1
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
            "{},{},{},{},{}",
            self.seed, self.n, self.exponent, self.max_start_time, self.max_duration_factor
        )
    }

    fn version(&self) -> u8 {
        1
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
                duration: Some(DurationRange::new(d_min as u32, d_max as u32)),
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
    start_times: TimeDistribution,
    durations: TimeDistribution,
    duration_starts: TimeDistribution,
    duration_durations: TimeDistribution,
}

impl RandomQueryset {
    pub fn new(
        seed: u64,
        n: usize,
        start_times: TimeDistribution,
        durations: TimeDistribution,
        duration_starts: TimeDistribution,
        duration_durations: TimeDistribution
    ) -> Self {
        Self {
            seed,
            n,
            start_times,
            durations,
            duration_starts,
            duration_durations
        }
    }
}

impl Queryset for RandomQueryset {
    fn name(&self) -> String {
        format!(
            "random-{}-{}-{}-{}",
            self.start_times.name(),
            self.durations.name(),
            self.duration_starts.name(),
            self.duration_durations.name()
        )
    }

    fn parameters(&self) -> String {
        format!(
            "{},{}_{}_{}_{}_{}",
            self.seed,
            self.n,
            self.start_times.parameters(),
            self.durations.parameters(),
            self.duration_starts.parameters(),
            self.duration_durations.parameters(),
        )
    }

    fn version(&self) -> u8 {
        2
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
        let mut start_times = self.start_times.stream(rng1);
        let mut durations = self.durations.stream(rng2);
        let mut duration_starts = self.duration_starts.stream(rng3);
        let mut duration_durations = self.duration_durations.stream(rng4);
        // for _ in 0..self.n {
        while data.len() < self.n {
            let interval = Interval::new(start_times.next().unwrap(), durations.next().unwrap());
            let duration_range_start = duration_starts.next().unwrap();
            let duration_range_duration = duration_durations.next().unwrap();
            let duration_range = DurationRange::new(
                duration_range_start,
                duration_range_start + duration_range_duration,
            );
            data.insert(Query {
                range: Some(interval),
                duration: Some(duration_range),
            });
        }
        data.into_iter().collect()
    }
}
