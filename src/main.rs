#[macro_use]
extern crate log;

mod dataset;
mod naive;
mod reporter;
mod types;
mod zipf;

use anyhow::{anyhow, Context, Result};
use argh::FromArgs;
use dataset::*;
use regex::*;
use types::*;

#[derive(FromArgs, Debug)]
/// Run experiments indexing temporal relations
pub struct Config {
    #[argh(option)]
    /// description of the dataset to use
    pub dataset: String,

    #[argh(option)]
    /// description of the queryset to use
    pub queries: String,

    #[argh(option)]
    /// the algorithm to test, including its parameters
    pub algorithm: String,

    #[argh(option, default = "false")]
    /// whether to resrun the given configuration
    pub rerun: bool,
}

impl Config {
    pub fn get_algorithm(&self) -> Result<Box<dyn Algorithm>> {
        match self.algorithm.as_ref() {
            "linear-scan" => Ok(Box::new(naive::LinearScan)),
            e => Err(anyhow!("unknown algorithm: {}", e)),
        }
    }

    pub fn get_queryset(&self) -> Result<Box<dyn Queryset>> {
        let re_zipf_and_uniform = Regex::new(
            r"^zipf-and-uniform\((\d+), *(\d+), *(\d+(:?\.\d+)?), *(\d+), *(\d+(:?\.\d+)?)\)$",
        )?;
        if let Some(captures) = re_zipf_and_uniform.captures(&self.queries) {
            trace!("Captures: {:?}", captures);
            let seed = captures[1].parse::<u64>().context("seed")?;
            let n = captures[2].parse::<usize>().context("n")?;
            let exponent = captures[3].parse::<f64>().context("exponent")?;
            let max_start_time = captures[5].parse::<u32>().context("max start time")?;
            let max_duration_factor = captures[6].parse::<f64>().context("max duration factor")?;
            return Ok(Box::new(RandomQueriesZipfAndUniform::new(
                seed,
                n,
                exponent,
                max_start_time,
                max_duration_factor,
            )));
        }

        Err(anyhow!("unknown queryset: {}", self.queries))
    }

    pub fn get_dataset(&self) -> Result<Box<dyn Dataset>> {
        let re_zipf_and_uniform =
            Regex::new(r"^zipf-and-uniform\((\d+), *(\d+), *(\d+(:?\.\d+)?), *(\d+)\)$")?;
        if let Some(captures) = re_zipf_and_uniform.captures(&self.dataset) {
            trace!("Captures: {:?}", captures);
            let seed = captures[1].parse::<u64>().context("seed")?;
            let n = captures[2].parse::<usize>().context("n")?;
            let exponent = captures[3].parse::<f64>().context("exponent")?;
            let max_start_time = captures[5].parse::<u32>().context("max start time")?;
            return Ok(Box::new(RandomDatasetZipfAndUniform::new(
                seed,
                n,
                exponent,
                max_start_time,
            )));
        }

        Err(anyhow!("unknown dataset: {}", self.dataset))
    }
}

fn main() -> Result<()> {
    use std::time::*;

    pretty_env_logger::init();
    reporter::db_setup()?;

    let config: Config = argh::from_env();

    let queryset = config.get_queryset()?;
    info!(
        "queryset: {} (v{}) {}",
        queryset.name(),
        queryset.version(),
        queryset.parameters()
    );
    let algorithm = config.get_algorithm()?;
    info!(
        "algorithm: {} (v{}) {}",
        algorithm.name(),
        algorithm.version(),
        algorithm.parameters()
    );
    let dataset = config.get_dataset()?;
    info!(
        "dataset: {} (v{}) {}",
        dataset.name(),
        dataset.version(),
        dataset.parameters()
    );

    let reporter = reporter::Reporter::new(config);
    if let Some(sha) = reporter.already_run()? {
        info!("parameter configuration already run: {}", sha);
        return Ok(())
    }

    let queryset_queries = queryset.get();
    let dataset_intervals = dataset.get();

    let start = Instant::now();
    algorithm.run(&dataset_intervals, &queryset_queries);
    let end = Instant::now();
    let elapsed = (end - start).as_millis() as i64; // truncation happens here, but only on extremely long runs

    reporter.report(elapsed)?;

    Ok(())
}
