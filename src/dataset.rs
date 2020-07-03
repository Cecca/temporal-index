extern crate rand;
extern crate rand_xoshiro;
//extern crate zipf;

use crate::types::*;
use crate::zipf::ZipfDistribution;
use rand::distributions::Distribution;
use rand::distributions::Uniform;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;
use std::collections::BTreeMap;

pub trait Dataset: std::fmt::Debug {
    fn name(&self) -> String;
    fn parameters(&self) -> String;
    fn get(&self) -> Vec<Interval>;
    fn version(&self) -> u8;

    fn stats(&self) -> Stats {
        Stats::new(&self.get())
    }
}

#[derive(Debug)]
pub struct Stats {
    pub start_percentiles: BTreeMap<usize, Time>,
    pub duration_percentiles: BTreeMap<usize, Time>,
}

impl Stats {
    fn new(intervals: &[Interval]) -> Self {
        let mut starts: Vec<Time> = intervals.iter().map(|i| i.start).collect();
        let mut durations: Vec<Time> = intervals.iter().map(|i| i.duration()).collect();
        starts.sort();
        durations.sort();
        let mut start_percentiles = BTreeMap::new();
        let mut duration_percentiles = BTreeMap::new();
        start_percentiles.insert(0, starts[0]);
        start_percentiles.insert(25, starts[starts.len() / 4]);
        start_percentiles.insert(50, starts[starts.len() / 2]);
        start_percentiles.insert(75, starts[3 * starts.len() / 4]);
        start_percentiles.insert(100, starts[starts.len() - 1]);
        duration_percentiles.insert(0, durations[0]);
        duration_percentiles.insert(25, durations[durations.len() / 4]);
        duration_percentiles.insert(50, durations[durations.len() / 2]);
        duration_percentiles.insert(75, durations[3 * durations.len() / 4]);
        duration_percentiles.insert(100, durations[durations.len() - 1]);

        Self {
            start_percentiles,
            duration_percentiles,
        }
    }

    pub fn log(&self) {
        info!("         {:>8} {:>8} {:>8} {:>8} {:>8}", "min", 25, 50, 75, "max");
        info!(
            "start    {:>8} {:>8} {:>8} {:>8} {:>8}",
            self.start_percentiles[&0],
            self.start_percentiles[&25],
            self.start_percentiles[&50],
            self.start_percentiles[&75],
            self.start_percentiles[&100]
        );
        info!(
            "duration {:>8} {:>8} {:>8} {:>8} {:>8}",
            self.duration_percentiles[&0],
            self.duration_percentiles[&25],
            self.duration_percentiles[&50],
            self.duration_percentiles[&75],
            self.duration_percentiles[&100]
        );
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
