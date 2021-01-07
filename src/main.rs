#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

mod algorithms;
mod configuration;
mod dataset;
mod reporter;
#[cfg(test)]
mod test;
mod types;
mod zipf;

use crate::configuration::*;
use anyhow::Result;
use argh::FromArgs;
use std::path::PathBuf;
use sysinfo::{System, SystemExt};

#[derive(FromArgs)]
/// run experiments on temporal indices
struct Cmdline {
    #[argh(switch)]
    /// force rerunning the experiments
    rerun: bool,

    #[argh(option)]
    /// print the histogram of dataset/queryset start/end times, durations and exits
    histogram: Option<String>,

    #[argh(option)]
    /// print the actual intervals in the dataset, or the queries
    dump: Option<String>,

    #[argh(positional)]
    /// the file containing the experiments to run
    experiment_file: PathBuf,
}

fn log_memory(system: &mut System) {
    system.refresh_memory();
    let swap = system.get_used_swap();
    if swap > 0 {
        warn!(
            "memory used: {} kB, free: {} kB, swap: {} kB",
            system.get_used_memory(),
            system.get_free_memory(),
            system.get_used_swap()
        );
    } else {
        info!(
            "memory used: {} kB, free: {} kB, swap: {} kB",
            system.get_used_memory(),
            system.get_free_memory(),
            system.get_used_swap()
        );
    }
}

fn main() -> Result<()> {
    use std::time::*;

    let mut system = System::new();
    pretty_env_logger::formatted_builder()
        .filter_level(log::LevelFilter::Info)
        .try_init()?;
    if std::env::args()
        .nth(1)
        .map(|s| s.eq("backup"))
        .unwrap_or(false)
    {
        reporter::Reporter::backup()?;
        return Ok(());
    }

    let cmdline: Cmdline = argh::from_env();

    reporter::db_setup()?;

    let conf_file_path = &cmdline.experiment_file;
    let conf_file = std::fs::File::open(conf_file_path)?;
    let configurations: Vec<Configuration> = serde_yaml::from_reader(conf_file)?;

    if let Some(hist_what) = cmdline.histogram {
        for configuration in configurations {
            configuration.print_histogram(hist_what.clone())?;
        }
        return Ok(());
    } else if let Some(dump_what) = cmdline.dump {
        for configuration in configurations {
            configuration.dump(dump_what.clone())?;
        }
        return Ok(());
    }

    for configurations in configurations {
        configurations.for_each(|experiment| {
            let reporter = reporter::Reporter::new(conf_file_path, experiment.clone())?;
            if !cmdline.rerun {
                if let Some(id) = reporter.already_run()? {
                    debug!("parameter configuration already run: {}, skipping", id);
                    return Ok(());
                }
            }

            log_memory(&mut system);
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

            let queryset_queries = experiment.queries.get();
            let dataset_intervals = experiment.dataset.get()?;

            match experiment.experiment_type {
                ExperimentType::Batch => {
                    let (elapsed_index, elapsed_run, index_size_bytes) = {
                        // we need to run into a block (with some code duplication) in order to satisfy runtime constraints
                        // on the RefCell which is borrowed mutable by the algorithm

                        let mut algorithm = experiment.algorithm.borrow_mut();
                        info!("Building index");
                        let start = Instant::now();
                        algorithm.index(&dataset_intervals);
                        let end = Instant::now();
                        let elapsed_index = (end - start).as_millis() as i64; // truncation happens here, but only on extremely long runs
                        let index_size_bytes = algorithm.index_size() as u32;

                        info!("Running queries [batch]");
                        let start = Instant::now();
                        let _matches = algorithm.run_batch(&queryset_queries);
                        let end = Instant::now();
                        let elapsed_run = (end - start).as_millis() as i64; // truncation happens here, but only on extremely long runs

                        info!(
                            "time for index {}ms, time for query {}ms",
                            elapsed_index, elapsed_run
                        );
                        // Clear up the index to free resources
                        algorithm.clear();
                        (elapsed_index, elapsed_run, index_size_bytes)
                    };

                    reporter.report_batch(elapsed_index, elapsed_run, index_size_bytes)?;
                }
                ExperimentType::Focus { samples } => {
                    let results = {
                        // we need to run into a block (with some code duplication) in order to satisfy runtime constraints
                        // on the RefCell which is borrowed mutable by the algorithm
                        let mut algorithm = experiment.algorithm.borrow_mut();

                        info!("Building index");
                        algorithm.index(&dataset_intervals);

                        info!("Running queries [focus with {} samples]", samples);
                        let results = algorithm.run_focus(&queryset_queries, samples);

                        // Clear up the index to free resources
                        algorithm.clear();
                        results
                    };

                    reporter.report_focus(results)?;
                }
            }

            Ok(())
        })?;
    }

    Ok(())
}
