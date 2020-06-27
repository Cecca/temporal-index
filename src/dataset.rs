extern crate rand;
extern crate rand_xoshiro;
//extern crate zipf;

use crate::types::*;
use crate::zipf::ZipfDistribution;
use rand::distributions::Distribution;
use rand::distributions::Uniform;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;

pub trait Dataset {
    fn name(&self) -> String;
    fn parameters(&self) -> String;
    fn get(&self) -> Vec<Interval>;
    fn version(&self) -> u8;
}

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
        String::from("RandomDatasetZipAndUniform")
    }

    fn parameters(&self) -> String {
        format!("{},{},{},{}", self.seed, self.n, self.exponent, self.max_time)
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
        data
    }
}

pub trait Queryset {
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
        String::from("RandomQueriesZipfAndUniform")
    }

    fn parameters(&self) -> String {
        format!("{},{},{},{},{}", self.seed, self.n, self.exponent, self.max_start_time, self.max_duration_factor)
    }

    fn version(&self) -> u8 {
        1
    }

    fn get(&self) -> Vec<Query> {
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(self.seed);
        let interval_duration_distribution = ZipfDistribution::new(self.n, self.exponent)
            .expect("problem creating Zipf distribution");
        let start_time_distribution = Uniform::new(0, self.max_start_time);
        let duration_factor_distribution = Uniform::new(0.1, self.max_duration_factor);
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
            let d_max = (d_max * range.duration() as f64) as Time;
            data.push(Query {
                range: Some(range),
                duration: Some(DurationRange::new(d_min, d_max)),
            });
        }
        data
    }
}
