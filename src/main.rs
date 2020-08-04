#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

mod btree;
mod configuration;
mod dataset;
mod grid;
mod grid3d;
mod interval_tree;
mod naive;
mod period_index;
mod reporter;
mod types;
mod zipf;
mod ebi;

use crate::configuration::*;
use anyhow::Result;
use argh::FromArgs;
use std::path::PathBuf;

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

fn main() -> Result<()> {
    use std::time::*;

    pretty_env_logger::init();
    let cmdline: Cmdline = argh::from_env();

    reporter::db_setup()?;

    let conf_file_path = &cmdline.experiment_file;
    let conf_file = std::fs::File::open(conf_file_path)?;
    let configurations: Configuration = serde_yaml::from_reader(conf_file)?;

    if let Some(hist_what) = cmdline.histogram {
        return configurations.print_histogram(hist_what);
    }

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
