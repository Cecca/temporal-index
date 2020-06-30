#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

mod dataset;
mod naive;
mod period_index;
mod reporter;
mod types;
mod zipf;

use anyhow::{anyhow, Context, Result};
use argh::FromArgs;
use dataset::*;
use itertools::iproduct;
use regex::*;
use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;
use types::*;

#[derive(Debug, Clone)]
pub struct ExperimentConfiguration {
    dataset: Rc<dyn Dataset>,
    queries: Rc<dyn Queryset>,
    algorithm: Rc<RefCell<dyn Algorithm>>,
}

impl std::fmt::Display for ExperimentConfiguration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}({}) {}({}) {}({})",
            self.dataset.name(),
            self.dataset.parameters(),
            self.queries.name(),
            self.queries.parameters(),
            self.algorithm.borrow().name(),
            self.algorithm.borrow().parameters()
        )
    }
}

#[derive(FromArgs, Debug)]
/// Run experiments indexing temporal relations
pub struct CmdlineConfig {
    #[argh(option)]
    /// description of the dataset to use
    pub dataset: String,

    #[argh(option)]
    /// description of the queryset to use
    pub queries: String,

    #[argh(option)]
    /// the algorithm to test, including its parameters
    pub algorithm: String,

    #[argh(switch)]
    /// whether to resrun the given configuration
    pub rerun: bool,
}

impl CmdlineConfig {
    pub fn get_configuration(&self) -> Result<ExperimentConfiguration> {
        // Ok(ExperimentConfiguration {
        //     dataset: self.get_dataset()?,
        //     queries: self.get_queryset()?,
        //     algorithm: self.get_algorithm()?,
        // })
        todo!()
    }

    pub fn get_algorithm(&self) -> Result<Box<dyn Algorithm>> {
        if self.algorithm == "linear-scan" {
            return Ok(Box::new(naive::LinearScan::new()));
        }
        let re_period_index = Regex::new(r"period-index\((\d+), *(\d+)\)")?;
        if let Some(captures) = re_period_index.captures(&self.algorithm) {
            trace!("algorithm parsing {:?}", captures);
            let bucket_length = captures[1].parse::<u32>()?;
            let num_levels = captures[2].parse::<u32>()?;
            return Ok(Box::new(period_index::PeriodIndex::new(
                bucket_length,
                num_levels,
            )?));
        }

        Err(anyhow!("unknown algorithm {}", self.algorithm))
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

#[derive(Serialize, Deserialize)]
enum AlgorithmConfiguration {
    PeriodIndex {
        bucket_length: Vec<u32>,
        num_levels: Vec<u32>,
    },
    LinearScan,
}

impl AlgorithmConfiguration {
    fn algorithms(&self) -> Box<dyn Iterator<Item = Rc<RefCell<dyn Algorithm>>> + '_> {
        match self {
            Self::PeriodIndex {
                bucket_length,
                num_levels,
            } => {
                let iter = iproduct!(bucket_length, num_levels).map(|(bl, nl)| {
                    Rc::new(RefCell::new(
                        period_index::PeriodIndex::new(*bl, *nl)
                            .expect("error in configured algorithm"),
                    )) as Rc<RefCell<dyn Algorithm>>
                });
                Box::new(iter)
            }
            Self::LinearScan => {
                let algo: Rc<RefCell<dyn Algorithm>> =
                    Rc::new(RefCell::new(naive::LinearScan::new()));
                Box::new(Some(algo).into_iter())
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
enum DataConfiguration {
    ZipfUniform {
        seed: Vec<u64>,
        n: Vec<usize>,
        exponent: Vec<f64>,
        max_start_time: Vec<u32>,
    },
}

impl DataConfiguration {
    fn datasets(&self) -> Box<dyn Iterator<Item = Rc<dyn Dataset>> + '_> {
        match self {
            Self::ZipfUniform {
                seed,
                n,
                exponent,
                max_start_time,
            } => {
                let iter = iproduct!(seed, n, exponent, max_start_time).map(
                    |(seed, n, exponent, max_start_time)| {
                        Rc::new(RandomDatasetZipfAndUniform::new(
                            *seed,
                            *n,
                            *exponent,
                            *max_start_time,
                        )) as Rc<dyn Dataset>
                    },
                );
                Box::new(iter)
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
enum QueryConfiguration {
    ZipfUniform {
        seed: Vec<u64>,
        n: Vec<usize>,
        exponent: Vec<f64>,
        max_start_time: Vec<u32>,
        max_duration_factor: Vec<f64>,
    },
}

impl QueryConfiguration {
    fn queries(&self) -> Box<dyn Iterator<Item = Rc<dyn Queryset>> + '_> {
        match self {
            Self::ZipfUniform {
                seed,
                n,
                exponent,
                max_start_time,
                max_duration_factor,
            } => {
                let iter = iproduct!(seed, n, exponent, max_start_time, max_duration_factor).map(
                    |(seed, n, exponent, max_start_time, max_duration_factor)| {
                        Rc::new(RandomQueriesZipfAndUniform::new(
                            *seed,
                            *n,
                            *exponent,
                            *max_start_time,
                            *max_duration_factor,
                        )) as Rc<dyn Queryset>
                    },
                );
                Box::new(iter)
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Configuration {
    datasets: Vec<DataConfiguration>,
    queries: Vec<QueryConfiguration>,
    algorithms: Vec<AlgorithmConfiguration>,
}

impl Configuration {
    fn for_each<F: FnMut(ExperimentConfiguration) -> Result<()>>(
        self,
        mut action: F,
    ) -> Result<()> {
        for dataset in self.datasets.iter().flat_map(|d| d.datasets()) {
            for queries in self.queries.iter().flat_map(|q| q.queries()) {
                for algorithm in self.algorithms.iter().flat_map(|a| a.algorithms()) {
                    let conf = ExperimentConfiguration {
                        dataset: Rc::clone(&dataset),
                        queries: Rc::clone(&queries),
                        algorithm: Rc::clone(&algorithm),
                    };
                    let dbg_str = format!("{:?}", conf);
                    action(conf).context(dbg_str)?
                }
            }
        }
        Ok(())
    }
}

#[derive(FromArgs)]
/// run experiments on temporal indices
struct Cmdline2 {
    #[argh(switch)]
    /// force rerunning the experiments
    rerun: bool,

    #[argh(positional)]
    /// the file containing the experiments to run
    experiment_file: PathBuf,
}

fn main() -> Result<()> {
    use std::time::*;

    pretty_env_logger::init();
    let cmdline: Cmdline2 = argh::from_env();

    reporter::db_setup()?;

    let conf_file_path = &cmdline.experiment_file;
    let conf_file = std::fs::File::open(conf_file_path)?;
    let configurations: Configuration = serde_yaml::from_reader(conf_file)?;

    configurations.for_each(|experiment| {
        info!("{:-<60}", "");
        info!(
            "{:>20} (v{}) {}",
            experiment.dataset.name(),
            experiment.dataset.version(),
            experiment.dataset.parameters()
        );
        info!(
            "{:>20} (v{}) {}",
            experiment.queries.name(),
            experiment.queries.version(),
            experiment.queries.parameters()
        );
        info!(
            "{:>20} (v{}) {}",
            experiment.algorithm.borrow().name(),
            experiment.algorithm.borrow().version(),
            experiment.algorithm.borrow().parameters()
        );

        let reporter = reporter::Reporter::new(conf_file_path, experiment.clone());
        if !cmdline.rerun {
            if let Some(sha) = reporter.already_run()? {
                info!(
                    "parameter configuration already run: {}, skipping",
                    sha
                );
                return Ok(());
            }
        }

        let (elapsed_index, elapsed_run, answers) = {
            let mut algorithm = experiment.algorithm.borrow_mut();

            let queryset_queries = experiment.queries.get();
            let dataset_intervals = experiment.dataset.get();

            let start = Instant::now();
            algorithm.index(&dataset_intervals);
            let end = Instant::now();
            let elapsed_index = (end - start).as_millis() as i64; // truncation happens here, but only on extremely long runs

            let start = Instant::now();
            let answers = algorithm.run(&queryset_queries);
            let end = Instant::now();
            let elapsed_run = (end - start).as_millis() as i64; // truncation happens here, but only on extremely long runs
            (elapsed_index, elapsed_run, answers)
        };

        info!("time for index {}ms, time for query {}ms", elapsed_index, elapsed_run);

        reporter.report(elapsed_index, elapsed_run, answers)?;

        Ok(())
    })?;

    Ok(())

}
