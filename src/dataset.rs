extern crate rand;
extern crate rand_xoshiro;
//extern crate zipf;

use crate::zipf::ZipfDistribution;
use crate::{algorithms::PeriodIndexPlusPlus, types::*};
use anyhow::Context;
use anyhow::Result;
use csv;
use progress_logger::ProgressLogger;
use rand::distributions::*;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;
use std::path::PathBuf;
use std::rc::Rc;

pub trait Dataset: std::fmt::Debug {
    fn name(&self) -> String;
    fn parameters(&self) -> String;
    fn get(&self) -> Result<Vec<Interval>>;
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

#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum TimeDistribution {
    Uniform {
        low: Time,
        high: Time,
    },
    Zipf {
        n: usize,
        beta: f64,
    },
    Clustered {
        n: usize,
        high: Time,
        std_dev: Time,
    },
    /// All the numbers sampled from the inner distribution are multiplied by the
    /// given scale factor
    Scaled {
        inner: Box<TimeDistribution>,
        scale: Time,
    },
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
            Self::Scaled { inner, scale } => {
                let rng = rng.clone();
                Box::new(inner.stream(rng).map(move |x| x * scale))
            }
        }
    }

    pub fn name(&self) -> String {
        match &self {
            Self::Uniform { .. } => String::from("uniform"),
            Self::Zipf { .. } => String::from("zipf"),
            Self::Clustered { .. } => String::from("clustered"),
            Self::Scaled { inner, .. } => format!("{}-scaled", inner.name()),
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
            Self::Scaled { inner, scale } => {
                format!("{} {}scale={}", inner.parameters(prefix), prefix, scale)
            }
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
    fn get(&self) -> Result<Vec<Interval>> {
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
        Ok(data)
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

    fn get(&self) -> Result<Vec<Interval>> {
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
        Ok(data)
    }
}

/// Takes a base dataset, and repeats all its intervals
/// shifted by multiples of the time span of the
/// original dataset. Useful for building scalability
/// benchmarks
#[derive(Debug)]
pub struct ReiteratedDataset {
    copies: usize,
    base: Vec<Interval>,
    base_name: String,
    base_params: String,
    base_version: u8,
}

impl ReiteratedDataset {
    pub fn new(dataset: Rc<dyn Dataset>, copies: usize) -> Result<Self> {
        let base = dataset.get()?;
        Ok(Self {
            copies,
            base,
            base_name: dataset.name(),
            base_params: dataset.parameters(),
            base_version: dataset.version(),
        })
    }
}

impl Dataset for ReiteratedDataset {
    fn name(&self) -> String {
        format!("reiterated-{}", self.base_name)
    }

    fn parameters(&self) -> String {
        format!(
            "copies={} base={} base_params=[{}] base_version={}",
            self.copies, self.base_name, self.base_params, self.base_version
        )
    }

    fn version(&self) -> u8 {
        1
    }

    fn get(&self) -> Result<Vec<Interval>> {
        let offset = self
            .base
            .iter()
            .map(|interval| interval.end)
            .max()
            .expect("maximum on empty vector")
            - self
                .base
                .iter()
                .map(|interval| interval.start)
                .min()
                .expect("minimum on empty vector");

        let res = (0..self.copies)
            .flat_map(|copy_idx| {
                self.base.iter().map(move |interval| Interval {
                    start: interval.start + copy_idx as u64 * offset,
                    end: interval.end + copy_idx as u64 * offset,
                })
            })
            .collect();
        Ok(res)
    }
}

pub struct QueryStats {
    pub selectivity: f64,
    pub selectivity_time: f64,
    pub selectivity_duration: f64,
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

    fn stats(&self, dataset: &[Interval]) -> Vec<(u32, QueryStats)> {
        let mut index = PeriodIndexPlusPlus::new(50);
        index.index(dataset);
        let queries = self.get();
        let mut pl = ProgressLogger::builder()
            .with_expected_updates(queries.len() as u64)
            .with_items_name("queries")
            .start();

        let res = queries
            .into_iter()
            .enumerate()
            .map(|(query_index, query)| {
                let selectivity_time = if let Some(range) = query.range {
                    let mut answer = QueryAnswer::builder();
                    index.query(
                        &Query {
                            range: Some(range),
                            duration: None,
                        },
                        &mut answer,
                    );
                    answer.finalize().num_matches() as f64 / dataset.len() as f64
                } else {
                    1.0
                };
                let selectivity_duration = if let Some(duration) = query.duration {
                    let mut answer = QueryAnswer::builder();
                    index.query(
                        &Query {
                            range: None,
                            duration: Some(duration),
                        },
                        &mut answer,
                    );
                    answer.finalize().num_matches() as f64 / dataset.len() as f64
                } else {
                    1.0
                };
                let selectivity = {
                    let mut answer = QueryAnswer::builder();
                    index.query(&query, &mut answer);
                    answer.finalize().num_matches() as f64 / dataset.len() as f64
                };
                pl.update(1u64);
                (
                    query_index as u32,
                    QueryStats {
                        selectivity,
                        selectivity_duration,
                        selectivity_time,
                    },
                )
            })
            .collect();

        pl.stop();
        res
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
    durations: Option<TimeDistribution>,
}

impl RandomQueryset {
    pub fn new(
        seed: u64,
        n: usize,
        intervals: Option<(TimeDistribution, TimeDistribution)>,
        durations: Option<TimeDistribution>,
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
                .clone()
                .map(|pair| format!("{}-{}", pair.0.name(), pair.1.name()))
                .unwrap_or("None".to_owned()),
            self.durations
                .clone()
                .map(|pair| format!("{}", pair.name()))
                .unwrap_or("None".to_owned()),
        )
    }

    fn parameters(&self) -> String {
        format!(
            "seed={} n={} {} {}",
            self.seed,
            self.n,
            self.intervals
                .clone()
                .map(|it| format!("{} {}", it.0.parameters("start_"), it.1.parameters("dur_")))
                .unwrap_or(String::new()),
            self.durations
                .clone()
                .map(|d| format!("{}", d.parameters("dur_dist_")))
                .unwrap_or(String::new()),
        )
    }

    fn version(&self) -> u8 {
        7
    }

    fn get(&self) -> Vec<Query> {
        use rand::RngCore;
        use std::collections::BTreeSet;
        let mut seeder = rand_xoshiro::SplitMix64::seed_from_u64(self.seed);
        let rng1 = Xoshiro256PlusPlus::seed_from_u64(seeder.next_u64());
        let rng2 = Xoshiro256PlusPlus::seed_from_u64(seeder.next_u64());
        let rng3 = Xoshiro256PlusPlus::seed_from_u64(seeder.next_u64());
        let mut data = BTreeSet::new();
        let mut interval_gen = self
            .intervals
            .as_ref()
            .map(move |(start_times, durations)| {
                (start_times.stream(rng1), durations.stream(rng2))
            });
        let mut durations_gen = self.durations.as_ref().map(move |d| d.stream(rng3));
        let mut cnt = 0;
        while data.len() < self.n && cnt < self.n * 2 {
            let interval = interval_gen.as_mut().map(|(start, duration)| {
                Interval::new(start.next().unwrap(), duration.next().unwrap())
            });
            let duration_range = durations_gen.as_mut().map(|d| {
                let a = d.next().unwrap();
                let b = d.next().unwrap();
                DurationRange::new(std::cmp::min(a, b), std::cmp::max(a, b))
            });
            data.insert(Query {
                range: interval,
                duration: duration_range,
            });
            cnt += 1;
        }
        assert!(
            data.len() == self.n,
            "Generated too few vectors ({} out of {})",
            data.len(),
            self.n,
        );
        data.into_iter().collect()
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RandomCappedQueryset {
    seed: u64,
    n: usize,
    /// Caps intervals to this time
    cap: Time,
    intervals: Option<(TimeDistribution, TimeDistribution)>,
    durations: Option<TimeDistribution>,
}

impl RandomCappedQueryset {
    pub fn new(
        seed: u64,
        n: usize,
        cap: Time,
        intervals: Option<(TimeDistribution, TimeDistribution)>,
        durations: Option<TimeDistribution>,
    ) -> Self {
        Self {
            seed,
            n,
            cap,
            intervals,
            durations,
        }
    }
}

impl Queryset for RandomCappedQueryset {
    fn name(&self) -> String {
        format!(
            "random-capped-{}-{}",
            self.intervals
                .clone()
                .map(|pair| format!("{}-{}", pair.0.name(), pair.1.name()))
                .unwrap_or("None".to_owned()),
            self.durations
                .clone()
                .map(|pair| format!("{}", pair.name()))
                .unwrap_or("None".to_owned()),
        )
    }

    fn parameters(&self) -> String {
        format!(
            "seed={} n={} cap={} {} {}",
            self.seed,
            self.n,
            self.cap,
            self.intervals
                .clone()
                .map(|it| format!("{} {}", it.0.parameters("start_"), it.1.parameters("dur_")))
                .unwrap_or(String::new()),
            self.durations
                .clone()
                .map(|d| format!("{}", d.parameters("dur_dist_")))
                .unwrap_or(String::new()),
        )
    }

    fn version(&self) -> u8 {
        1
    }

    fn get(&self) -> Vec<Query> {
        use rand::RngCore;
        use std::collections::BTreeSet;
        let mut seeder = rand_xoshiro::SplitMix64::seed_from_u64(self.seed);
        let rng1 = Xoshiro256PlusPlus::seed_from_u64(seeder.next_u64());
        let rng2 = Xoshiro256PlusPlus::seed_from_u64(seeder.next_u64());
        let rng3 = Xoshiro256PlusPlus::seed_from_u64(seeder.next_u64());
        let mut data = BTreeSet::new();
        let mut interval_gen = self
            .intervals
            .as_ref()
            .map(move |(start_times, durations)| {
                (start_times.stream(rng1), durations.stream(rng2))
            });
        let mut durations_gen = self.durations.as_ref().map(move |d| d.stream(rng3));
        let mut cnt = 0;
        while data.len() < self.n && cnt < self.n * 2 {
            let interval = interval_gen
                .as_mut()
                .map(|(start, duration)| {
                    Interval::new(start.next().unwrap(), duration.next().unwrap())
                })
                .map(|interval| {
                    if interval.end > self.cap {
                        assert!(
                            interval.start < self.cap,
                            "interval.start={}, self.cap={}",
                            interval.start,
                            self.cap
                        );
                        Interval {
                            start: interval.start,
                            end: self.cap,
                        }
                    } else {
                        interval
                    }
                });
            let duration_range = durations_gen.as_mut().map(|d| {
                let a = d.next().unwrap();
                let b = d.next().unwrap();
                DurationRange::new(std::cmp::min(a, b), std::cmp::max(a, b))
            });
            data.insert(Query {
                range: interval,
                duration: duration_range,
            });
            cnt += 1;
        }
        assert!(
            data.len() == self.n,
            "Generated too few vectors ({} out of {})",
            data.len(),
            self.n,
        );
        data.into_iter().collect()
    }
}

#[derive(Debug)]
pub struct SystematicQueryset {
    seed: u64,
    n: usize,
    base: Vec<Interval>,
    base_name: String,
    base_params: String,
    base_version: u8,
}

impl SystematicQueryset {
    pub fn new(seed: u64, n: usize, base: &Rc<dyn Dataset>) -> Self {
        Self {
            seed,
            n,
            base: base.get().expect("problems getting the vectors"),
            base_name: base.name(),
            base_params: base.parameters(),
            base_version: base.version(),
        }
    }
}

impl Queryset for SystematicQueryset {
    fn name(&self) -> String {
        "systematic".to_owned()
    }

    fn parameters(&self) -> String {
        format!(
            "seed={} n={} base=[{} {} {}]",
            self.seed, self.n, self.base_name, self.base_version, self.base_params
        )
    }

    fn version(&self) -> u8 {
        1
    }

    fn get(&self) -> Vec<Query> {
        use std::collections::BTreeMap;
        let side = (self.n as f64).sqrt().ceil();

        let base_size = self.base.len();
        let step = (base_size as f64 / side).ceil() as usize;

        // Collect information about the dataset
        let mut durations: Vec<Time> = self
            .base
            .iter()
            .map(|interval| interval.duration())
            .collect();
        durations.sort();
        let min_start_time = self
            .base
            .iter()
            .map(|interval| interval.start)
            .min()
            .expect("min in empty vec");
        let max_end_time = self
            .base
            .iter()
            .map(|interval| interval.end)
            .max()
            .expect("max in empty vec");

        let mut index = crate::algorithms::PeriodIndexPlusPlus::new(100);
        index.index(&self.base);

        // Function to snap the selectivity of randomly generated queries to a grid side*side
        let snap = |matching_n: u32| {
            let sel = matching_n as f64 / base_size as f64;
            (sel * side).ceil() as usize
        };

        let gen_t = Uniform::new(min_start_time, max_end_time);
        let gen_idx_d = Uniform::new(0, durations.len());
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(self.seed);

        // first, let's generate the time ranges at random
        let mut ranges = BTreeMap::new();
        let mut cnt = 0;
        while ranges.len() < side as usize && cnt < 50 * side as usize {
            let t1 = gen_t.sample(&mut rng);
            let t2 = gen_t.sample(&mut rng);
            let interval = Interval {
                start: std::cmp::min(t1, t2),
                end: std::cmp::max(t1, t2),
            };
            let mut ans = QueryAnswer::builder();
            index.query(
                &Query {
                    range: Some(interval),
                    duration: None,
                },
                &mut ans,
            );
            let matching = ans.finalize().num_matches();
            ranges.entry(snap(matching)).or_insert(interval);
            cnt += 1;
        }
        assert!(
            ranges.len() == side as usize,
            "not enough intervals generated ({} out of {})",
            ranges.len(),
            side as usize
        );

        // now generate the durations
        let mut duration_ranges = BTreeMap::new();
        for sel_parameter in 1..=(side as usize) {
            let steps = sel_parameter * step;

            let mut tentatives = 100usize;

            while tentatives > 0 {
                let mut idx1 = gen_idx_d.sample(&mut rng);
                // slide the first index until the first element with that duration
                let d_reference = durations[idx1];
                while idx1 > 0 && durations[idx1 - 1] == d_reference {
                    idx1 -= 1;
                }

                let idx2 = std::cmp::min(idx1 + steps, durations.len() - 1);
                while idx2 - idx1 < steps && idx1 > 0 {
                    idx1 -= 1;
                }

                let range = DurationRange {
                    min: durations[idx1],
                    max: durations[idx2],
                };

                let mut ans = QueryAnswer::builder();
                index.query(
                    &Query {
                        range: None,
                        duration: Some(range),
                    },
                    &mut ans,
                );
                let matching = ans.finalize().num_matches();
                let snapped = snap(matching);
                duration_ranges.entry(snapped).or_insert(range);
                if snapped == sel_parameter {
                    break;
                } else {
                    tentatives -= 1;
                }
            }
        }

        ranges
            .values()
            .flat_map(|range: &Interval| {
                duration_ranges
                    .values()
                    .map(move |duration: &DurationRange| Query {
                        range: Some(*range),
                        duration: Some(*duration),
                    })
            })
            .collect()
    }

    fn descr(&self) -> String {
        format!(
            "{} ({}) [v{}]",
            self.name(),
            self.parameters(),
            self.version()
        )
    }

    fn stats(&self, dataset: &[Interval]) -> Vec<(u32, QueryStats)> {
        let mut index = PeriodIndexPlusPlus::new(50);
        index.index(dataset);
        let queries = self.get();
        let mut pl = ProgressLogger::builder()
            .with_expected_updates(queries.len() as u64)
            .with_items_name("queries")
            .start();

        let res = queries
            .into_iter()
            .enumerate()
            .map(|(query_index, query)| {
                let selectivity_time = if let Some(range) = query.range {
                    let mut answer = QueryAnswer::builder();
                    index.query(
                        &Query {
                            range: Some(range),
                            duration: None,
                        },
                        &mut answer,
                    );
                    answer.finalize().num_matches() as f64 / dataset.len() as f64
                } else {
                    1.0
                };
                let selectivity_duration = if let Some(duration) = query.duration {
                    let mut answer = QueryAnswer::builder();
                    index.query(
                        &Query {
                            range: None,
                            duration: Some(duration),
                        },
                        &mut answer,
                    );
                    answer.finalize().num_matches() as f64 / dataset.len() as f64
                } else {
                    1.0
                };
                let selectivity = {
                    let mut answer = QueryAnswer::builder();
                    index.query(&query, &mut answer);
                    answer.finalize().num_matches() as f64 / dataset.len() as f64
                };
                pl.update(1u64);
                (
                    query_index as u32,
                    QueryStats {
                        selectivity,
                        selectivity_duration,
                        selectivity_time,
                    },
                )
            })
            .collect();

        pl.stop();
        res
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

    fn get(&self) -> Result<Vec<Interval>> {
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
                    .unwrap_or_else(|_| panic!("problem parsing start time {:?}", record));
                let end = record
                    .get(self.end_column)
                    .unwrap()
                    .parse::<u64>()
                    .unwrap_or_else(|_| panic!("problem parsing end time {:?}", record));
                assert!(end >= start, "start={} end={}", start, end);
                (start, end + 1)
            })
            .collect();

        let min_time = pairs.iter().map(|p| p.0).min().unwrap();
        Ok(pairs
            .iter()
            .map(|(s, e)| Interval {
                start: *s - min_time,
                end: *e - min_time,
            })
            .collect())
    }

    fn version(&self) -> u8 {
        1
    }
}

fn maybe_download(source: &str, dest: PathBuf) -> Result<()> {
    // Curl has way less dependencies than reqwest, but has issues compiling, sometimes
    // use curl::easy::Easy;

    if !dest.is_file() {
        info!("Downloading `{}` to `{:?}`", source, dest);
        let mut output = std::fs::File::create(dest)?;
        let mut content = reqwest::blocking::get(source)?;
        std::io::copy(&mut content, &mut output)?;
        // let mut handle = Easy::new();
        // handle.url(source)?;
        // handle.write_function(move |data| {
        //     output.write(data).unwrap();
        //     Ok(data.len())
        // })?;
        // handle.perform()?;
    }
    Ok(())
}

/// Dataset of flights, using information about date and hour of actual departure.
/// The times are encoded as minutes elapsed from the earliest flight in the dataset.
/// Skips rows with missing values.
#[derive(Debug)]
pub struct FlightDataset {
    csv_path: PathBuf,
}

impl FlightDataset {
    pub fn from_upstream() -> Result<Self> {
        let dir = PathBuf::from(".datasets");
        if !dir.is_dir() {
            std::fs::create_dir(&dir)?;
        }
        let cache = dir.join("flights.csv.gz");
        maybe_download(
            "https://dl.dropboxusercontent.com/s/2ql6xmu613lt5l3/august_2018_nationwide.csv.gz",
            cache.clone(),
        )?;
        Ok(Self { csv_path: cache })
    }

    fn str_to_timepair(s: &str) -> Result<(u32, u32)> {
        let minutes: u32 = std::str::from_utf8(
            &s.as_bytes()
                .iter()
                .rev()
                .take(2)
                .rev()
                .copied()
                .collect::<Vec<u8>>(),
        )?
        .parse()?;
        if s.len() <= 2 {
            Ok((0, minutes))
        } else {
            let hours: u32 = std::str::from_utf8(
                &s.as_bytes()
                    .iter()
                    .rev()
                    .skip(2)
                    .rev()
                    .copied()
                    .collect::<Vec<u8>>(),
            )?
            .parse()?;
            let hours = if hours == 24 { 0 } else { hours };
            Ok((hours, minutes))
        }
    }

    fn str_to_date(s: &str) -> Result<chrono::Date<chrono::Utc>> {
        use chrono::prelude::*;
        let b = s.as_bytes();
        let year: i32 = std::str::from_utf8(&b[0..4])?
            .parse()
            .context("parse year")?;
        let month: u32 = std::str::from_utf8(&b[5..7])?
            .parse()
            .context("parse month")?;
        let day: u32 = std::str::from_utf8(&b[8..10])?
            .parse()
            .context("parse day")?;
        Ok(Utc.ymd(year, month, day))
    }
}

impl Dataset for FlightDataset {
    fn name(&self) -> String {
        "Flight".to_owned()
    }

    fn parameters(&self) -> String {
        "".to_owned()
    }

    fn get(&self) -> Result<Vec<Interval>> {
        use flate2::read::GzDecoder;
        let gzip_reader = GzDecoder::new(std::fs::File::open(&self.csv_path)?);
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .delimiter(b',')
            .from_reader(gzip_reader);

        let mut pairs = Vec::new();

        let header = reader.headers()?;
        trace!("{:?}", header);
        let flight_date_idx = header
            .iter()
            .enumerate()
            .find(|p| p.1 == "fl_date")
            .context("looking for fl_date in header")?
            .0;
        let dep_time_idx = header
            .iter()
            .enumerate()
            .find(|p| p.1 == "dep_time")
            .context("looking for dep_time in header")?
            .0;
        let elapsed_idx = header
            .iter()
            .enumerate()
            .find(|p| p.1 == "actual_elapsed_time")
            .context("looking for actual_elapsed_time in header")?
            .0;

        for record in reader.records() {
            let record = record?;
            trace!(
                "{}, {}, {}",
                record.get(flight_date_idx).unwrap(),
                record.get(dep_time_idx).unwrap(),
                record.get(elapsed_idx).unwrap()
            );
            let flight_date_str = record.get(flight_date_idx).context("flight date")?;
            let dep_time_str = record.get(dep_time_idx).context("dep_time")?;
            let elapsed_str = record.get(elapsed_idx).context("elapsed")?;

            // we skip not available values
            if !flight_date_str.is_empty() && !dep_time_str.is_empty() && !elapsed_str.is_empty() {
                let flight_date = Self::str_to_date(flight_date_str)?;
                let dep_time = Self::str_to_timepair(dep_time_str)?;
                let elapsed =
                    chrono::Duration::minutes(elapsed_str.parse::<f64>().with_context(|| {
                        format!(
                            "string to parse {}\n{:?}",
                            record.get(elapsed_idx).unwrap(),
                            record
                        )
                    })? as i64);

                let start = flight_date.and_hms(dep_time.0, dep_time.1, 0);
                let end = start + elapsed;

                assert!(start < end);
                pairs.push((start, end));
            }
        }

        let earliest_time = pairs
            .iter()
            .map(|p| p.0)
            .min()
            .context("min on empty vector")?;

        Ok(pairs
            .into_iter()
            .map(|(start, end)| {
                let start = (start - earliest_time).num_minutes() as u64;
                let end = (end - earliest_time).num_minutes() as u64;
                assert!(start < end);
                Interval { start, end }
            })
            .collect())
    }

    fn version(&self) -> u8 {
        1
    }
}

#[test]
fn test_str_to_time() {
    assert_eq!(FlightDataset::str_to_timepair("18").unwrap(), (0, 18));
    assert_eq!(FlightDataset::str_to_timepair("1130").unwrap(), (11, 30));
    assert_eq!(FlightDataset::str_to_timepair("130").unwrap(), (1, 30));
    assert_eq!(FlightDataset::str_to_timepair("111").unwrap(), (1, 11));
}

#[test]
fn test_str_to_date() {
    use chrono::prelude::*;
    assert_eq!(
        FlightDataset::str_to_date("2018-08-01").unwrap(),
        Utc.ymd(2018, 08, 01)
    );
}

#[derive(Debug)]
pub struct WebkitDataset {
    csv_path: PathBuf,
}

impl WebkitDataset {
    pub fn from_upstream() -> Result<Self> {
        let dir = PathBuf::from(".datasets");
        if !dir.is_dir() {
            std::fs::create_dir(&dir)?;
        }
        let cache = dir.join("webkit.csv.gz");
        maybe_download(
            "https://dl.dropboxusercontent.com/s/c6d53xvivn98nba/webkit.txt.gz?dl=0",
            cache.clone(),
        )?;
        Ok(Self { csv_path: cache })
    }
}

impl Dataset for WebkitDataset {
    fn name(&self) -> String {
        "Webkit".to_owned()
    }

    fn parameters(&self) -> String {
        "".to_owned()
    }

    fn get(&self) -> Result<Vec<Interval>> {
        use flate2::read::GzDecoder;
        let gzip_reader = GzDecoder::new(std::fs::File::open(&self.csv_path)?);
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .delimiter(b'\t')
            .from_reader(gzip_reader);

        let mut pairs = Vec::new();

        for record in reader.records() {
            let record = record?;
            let start_str = record.get(0).context("start")?;
            let end_str = record.get(1).context("end")?;

            // we skip not available values
            if !start_str.is_empty() && !end_str.is_empty() {
                let start: u64 = start_str.parse()?;
                let end: u64 = end_str.parse()?;
                let end = end + 1;
                assert!(start < end);
                pairs.push((start, end));
            }
        }

        let earliest_time = pairs
            .iter()
            .map(|p| p.0)
            .min()
            .context("min on empty vector")?;

        Ok(pairs
            .into_iter()
            .map(|(start, end)| {
                let start = start - earliest_time;
                let end = end - earliest_time;
                assert!(start < end);
                Interval { start, end }
            })
            .collect())
    }

    fn version(&self) -> u8 {
        1
    }
}

/// Subset of the medical dataset Mimic III
#[derive(Debug)]
pub struct MimicIIIDataset {
    csv_path: PathBuf,
}

impl MimicIIIDataset {
    pub fn from_upstream() -> Result<Self> {
        let dir = PathBuf::from(".datasets");
        if !dir.is_dir() {
            std::fs::create_dir(&dir)?;
        }
        let cache = dir.join("mimic-III.csv.gz");
        maybe_download(
            "https://dl.dropboxusercontent.com/s/8avhvpqlpb877og/MIMICIII-prescriptions.csv.gz?dl=0",
            cache.clone(),
        )?;
        Ok(Self { csv_path: cache })
    }
}

impl Dataset for MimicIIIDataset {
    fn name(&self) -> String {
        "MimicIII".to_owned()
    }

    fn parameters(&self) -> String {
        "".to_owned()
    }

    fn get(&self) -> Result<Vec<Interval>> {
        use flate2::read::GzDecoder;
        let gzip_reader = GzDecoder::new(std::fs::File::open(&self.csv_path)?);
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .delimiter(b',')
            .from_reader(gzip_reader);

        let header = reader.headers()?;
        let start_idx = 0;
        let end_idx = 1;

        let mut intervals = Vec::new();

        for record in reader.records() {
            let record = record?;
            let start = record.get(start_idx).context("start")?.parse::<u64>()?;
            let end = record.get(end_idx).context("start")?.parse::<u64>()?;
            intervals.push(Interval { start, end });
        }

        // There is an outlier interval with start time 0, we remove it.
        let intervals: Vec<Interval> = intervals.into_iter().filter(|i| i.start != 0).collect();

        // And now we shift all the other intervals
        let min_start = intervals
            .iter()
            .map(|i| i.start)
            .min()
            .context("min on empty collection")?;

        let intervals: Vec<Interval> = intervals
            .into_iter()
            .map(|i| Interval {
                start: i.start - min_start,
                end: i.end - min_start,
            })
            .collect();

        Ok(intervals)
    }

    fn version(&self) -> u8 {
        1
    }
}

/// Dataset of tourism stays in town. Records the arrival/departure date.
/// We encode these timestamps as offsets from the earliest arrival in the dataset,
/// do the granularity of the time domain is days.
#[derive(Debug)]
pub struct TourismDataset {
    csv_path: PathBuf,
}

impl TourismDataset {
    pub fn from_upstream() -> Result<Self> {
        let dir = PathBuf::from(".datasets");
        if !dir.is_dir() {
            std::fs::create_dir(&dir)?;
        }
        let cache = dir.join("tourism.csv.gz");
        maybe_download(
            "https://dl.dropboxusercontent.com/s/y7niqvht12ee05v/tourismdata.csv.gz?dl=0",
            cache.clone(),
        )?;
        Ok(Self { csv_path: cache })
    }

    fn str_to_date(s: &str) -> Result<chrono::Date<chrono::Utc>> {
        use chrono::prelude::*;
        let b = s.as_bytes();
        let year: i32 = std::str::from_utf8(&b[0..4])?
            .parse()
            .context("parse year")?;
        let month: u32 = std::str::from_utf8(&b[5..7])?
            .parse()
            .context("parse month")?;
        let day: u32 = std::str::from_utf8(&b[8..10])?
            .parse()
            .context("parse day")?;
        Ok(Utc.ymd(year, month, day))
    }
}

impl Dataset for TourismDataset {
    fn name(&self) -> String {
        "Tourism".to_owned()
    }

    fn parameters(&self) -> String {
        "".to_owned()
    }

    fn get(&self) -> Result<Vec<Interval>> {
        use flate2::read::GzDecoder;
        let gzip_reader = GzDecoder::new(std::fs::File::open(&self.csv_path)?);
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .delimiter(b',')
            .from_reader(gzip_reader);

        let header = reader.headers()?;
        trace!("{:?}", header);
        let arrival_idx = header
            .iter()
            .enumerate()
            .find(|p| p.1 == "arrival")
            .context("looking for arrival in header")?
            .0;
        let departure_idx = header
            .iter()
            .enumerate()
            .find(|p| p.1 == "departure")
            .context("looking for departure in header")?
            .0;

        let mut dates = Vec::new();

        for record in reader.records() {
            let record = record?;
            let arrival_str = record.get(arrival_idx).context("arrival")?;
            let departure_str = record.get(departure_idx).context("departure date")?;

            // we skip not available values
            if !arrival_str.is_empty() && !departure_str.is_empty() {
                let arrival_date = Self::str_to_date(arrival_str)?;
                let departure_date = Self::str_to_date(departure_str)?;
                dates.push((arrival_date, departure_date));
            }
        }

        let earliest_date = dates
            .iter()
            .map(|p| p.0)
            .min()
            .context("min on empty vector")?;

        Ok(dates
            .into_iter()
            .map(|(arrival, departure)| {
                let start = (arrival - earliest_date).num_days() as u64;
                let end = 1u64 + (departure - earliest_date).num_days() as u64;
                assert!(start < end);
                Interval { start, end }
            })
            .collect())
    }

    fn version(&self) -> u8 {
        1
    }
}

#[test]
fn test_date_parse() {
    use chrono::prelude::*;
    assert_eq!(
        TourismDataset::str_to_date("2015-08-09").unwrap(),
        Utc.ymd(2015, 08, 09)
    );
}
