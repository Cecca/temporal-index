#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

mod btree;
mod configuration;
mod dataset;
mod ebi;
mod grid;
mod grid3d;
mod interval_tree;
mod naive;
mod nested_btree;
mod nested_vecs;
mod period_index;
mod period_index_plusplus;
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
    }

    for configurations in configurations {
        configurations.for_each(|experiment| {
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

            let reporter = reporter::Reporter::new(conf_file_path, experiment.clone());
            if !cmdline.rerun {
                if let Some(sha) = reporter.already_run()? {
                    info!(
                        "parameter configuration already run: {}, skipping",
                        &sha[0..6]
                    );
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
    }

    Ok(())
}
