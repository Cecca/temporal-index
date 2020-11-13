extern crate rand;
extern crate rand_xoshiro;
//extern crate zipf;

use crate::types::*;
use crate::zipf::ZipfDistribution;
use anyhow::Context;
use anyhow::Result;
use csv;
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
pub struct RandomGranuleQueryset {
    seed: u64,
    n: usize,
    granule: u64,
    intervals: Option<(TimeDistribution, TimeDistribution)>,
    /// Define the distribution of both the start and the end point of the duration range
    durations: Option<TimeDistribution>,
}

impl RandomGranuleQueryset {
    pub fn new(
        seed: u64,
        n: usize,
        granule: u64,
        intervals: Option<(TimeDistribution, TimeDistribution)>,
        durations: Option<TimeDistribution>,
    ) -> Self {
        Self {
            seed,
            n,
            granule,
            intervals,
            durations,
        }
    }
}

impl Queryset for RandomGranuleQueryset {
    fn name(&self) -> String {
        format!(
            "random-granules-{}-{}",
            self.intervals
                .map(|pair| format!("{}-{}", pair.0.name(), pair.1.name()))
                .unwrap_or("None".to_owned()),
            self.durations
                .map(|d| format!("{}", d.name()))
                .unwrap_or("None".to_owned()),
        )
    }

    fn parameters(&self) -> String {
        format!(
            "seed={} n={} granule={} {} {}",
            self.seed,
            self.n,
            self.granule,
            self.intervals
                .map(|it| format!("{} {}", it.0.parameters("start_"), it.1.parameters("dur_")))
                .unwrap_or(String::new()),
            self.durations
                .map(|d| format!("{}", d.parameters("durrange_"),))
                .unwrap_or(String::new()),
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
        let mut data = BTreeSet::new();
        let mut interval_gen = self
            .intervals
            .as_ref()
            .map(move |(start_times, durations)| {
                (start_times.stream(rng1), durations.stream(rng2))
            });
        let mut durations_gen = self.durations.as_ref().map(move |dgen| dgen.stream(rng3));
        while data.len() < self.n {
            let interval = interval_gen.as_mut().map(|(start, duration)| {
                Interval::new(
                    start.next().unwrap() * self.granule,
                    duration.next().unwrap() * self.granule,
                )
            });
            let duration_range = durations_gen.as_mut().map(|dgen| {
                let a = dgen.next().unwrap() * self.granule;
                let b = dgen.next().unwrap() * self.granule;
                DurationRange::new(std::cmp::min(a, b), std::cmp::max(a, b))
            });
            data.insert(Query {
                range: interval,
                duration: duration_range,
            });
        }
        data.into_iter().collect()
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
