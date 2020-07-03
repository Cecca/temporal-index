#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

mod btree;
mod dataset;
mod interval_tree;
mod naive;
mod period_index;
mod reporter;
mod types;
mod zipf;

use anyhow::{Context, Result};
use argh::FromArgs;
use dataset::*;
use itertools::iproduct;
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

#[derive(Serialize, Deserialize)]
enum AlgorithmConfiguration {
    PeriodIndex {
        num_buckets: Vec<usize>,
        num_levels: Vec<u32>,
    },
    LinearScan,
    BTree,
    IntervalTree,
}

impl AlgorithmConfiguration {
    fn algorithms(&self) -> Box<dyn Iterator<Item = Rc<RefCell<dyn Algorithm>>> + '_> {
        match self {
            Self::PeriodIndex {
                num_buckets,
                num_levels,
            } => {
                let iter = iproduct!(num_buckets, num_levels).map(|(nb, nl)| {
                    Rc::new(RefCell::new(
                        period_index::PeriodIndex::new(*nb, *nl)
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
            Self::BTree => {
                let algo: Rc<RefCell<dyn Algorithm>> = Rc::new(RefCell::new(btree::BTree::new()));
                Box::new(Some(algo).into_iter())
            }
            Self::IntervalTree => {
                let algo: Rc<RefCell<dyn Algorithm>> =
                    Rc::new(RefCell::new(interval_tree::IntervalTree::new()));
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
            info!("{:=<60}", "");
            info!("{} (v{}) {}", dataset.name(), dataset.version(), dataset.parameters());
            dataset.stats().log();
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
struct Cmdline {
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
    let cmdline: Cmdline = argh::from_env();

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
                info!("parameter configuration already run: {}, skipping", sha);
                return Ok(());
            }
        }

        let (elapsed_index, elapsed_run, index_size_bytes, answers) = {
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
            // Clear up the index to free resources
            algorithm.clear();
            (
                elapsed_index,
                elapsed_run,
                algorithm.index_size() as u32,
                answers,
            )
        };

        info!(
            "time for index {}ms, time for query {}ms",
            elapsed_index, elapsed_run
        );

        reporter.report(elapsed_index, elapsed_run, index_size_bytes, answers)?;

        Ok(())
    })?;

    Ok(())
}
