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
use anyhow::{Context, Result};
use argh::FromArgs;
use std::path::PathBuf;
use sysinfo::{System, SystemExt};

use std::alloc::{GlobalAlloc, Layout, System as SystemAlloc};
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

/// We wrap the system allocator in order to count the number of bytes that are allocated,
/// so to instrument the size of the indices
struct CountingAllocator;

static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ret = SystemAlloc.alloc(layout);
        if !ret.is_null() {
            ALLOCATED.fetch_add(layout.size(), SeqCst);
        }
        return ret;
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        SystemAlloc.dealloc(ptr, layout);
        ALLOCATED.fetch_sub(layout.size(), SeqCst);
    }
}

#[global_allocator]
static A: CountingAllocator = CountingAllocator;

pub fn get_allocated() -> Bytes {
    Bytes(ALLOCATED.load(SeqCst))
}

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

#[derive(Serialize, Eq, PartialEq, Hash)]
struct WorkloadEntry {
    data_name: String,
    data_descr: String,
    data_path: PathBuf,
    data_version: u8,
    query_name: String,
    query_descr: String,
    query_path: PathBuf,
    query_version: u8,
}

fn export_workload(configurations: Vec<Configuration>, out_dir: &str) -> Result<()> {
    let out_dir = PathBuf::from(out_dir);
    if !out_dir.is_dir() {
        std::fs::create_dir_all(&out_dir)?;
    }
    let data_dir = out_dir.join("data");
    if !data_dir.is_dir() {
        std::fs::create_dir_all(&data_dir)?;
    }
    let queries_dir = out_dir.join("queries");
    if !queries_dir.is_dir() {
        std::fs::create_dir_all(&queries_dir)?;
    }
    let workload_file = out_dir.join("workload.json");

    let mut descriptions = std::collections::HashSet::new();

    for conf in configurations {
        conf.for_each(|experiment| {
            let data_path = experiment.dataset.to_csv(data_dir.clone())?;
            let queries_path = experiment.queries.to_csv(queries_dir.clone())?;
            descriptions.insert(WorkloadEntry {
                data_name: experiment.dataset.name(),
                data_descr: experiment.dataset.parameters(),
                data_path,
                data_version: experiment.dataset.version(),
                query_name: experiment.queries.name(),
                query_descr: experiment.queries.parameters(),
                query_path: queries_path,
                query_version: experiment.queries.version(),
            });
            Ok(())
        })?;
    }

    let writer = std::fs::File::create(workload_file)?;
    serde_json::to_writer(writer, &descriptions)?;

    Ok(())
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
        reporter::Reporter::backup(None)?;
        return Ok(());
    }
    if std::env::args()
        .nth(1)
        .map(|s| s.eq("export_example"))
        .unwrap_or(false)
    {
        crate::algorithms::rd_index::RDIndex::export_example()?;
        return Ok(());
    }
    if std::env::args()
        .nth(1)
        .map(|s| s.eq("export_data"))
        .unwrap_or(false)
    {
        let conf_file_path = std::env::args()
            .nth(2)
            .context("missing path to configuration file")?;
        let out_dir = std::env::args()
            .nth(3)
            .context("missing path to output directory")?;
        let conf_file = std::fs::File::open(&conf_file_path)?;
        let configurations: Vec<Configuration> = serde_yaml::from_reader(conf_file)?;
        export_workload(configurations, &out_dir)?;
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
                if let Some(id) = reporter.already_run(experiment.mode.clone())? {
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

            match experiment.mode {
                ExperimentMode::Parallel => {
                    let threads = rayon::current_num_threads();

                    let (elapsed_index, elapsed_run, index_size_bytes) = {
                        // we need to run into a block (with some code duplication) in order to satisfy runtime constraints
                        // on the RefCell which is borrowed mutable by the algorithm

                        let mut algorithm = experiment.algorithm.borrow_mut();
                        info!("Building index");
                        let start = Instant::now();
                        algorithm.index(&dataset_intervals);
                        let end = Instant::now();
                        let elapsed_index = (end - start).as_millis() as i64; // truncation happens here, but only on extremely long runs
                        info!("Index built in {:?}", end - start);

                        info!("Running queries [parallel batch]");
                        let start = Instant::now();
                        let matches = algorithm.run_parallel(&queryset_queries, threads);
                        let end = Instant::now();
                        let elapsed_run = (end - start).as_millis() as i64; // truncation happens here, but only on extremely long runs

                        info!(
                            "time for index {}ms, time for query {}ms, output size {}",
                            elapsed_index, elapsed_run, matches
                        );
                        // Clear up the index to free resources, and measure the space it took up
                        let allocated_start = get_allocated();
                        algorithm.clear();
                        let allocated_end = get_allocated();
                        assert!(allocated_start.0 > allocated_end.0);
                        info!("Size of the index was {}", allocated_start - allocated_end);
                        let index_size_bytes = (allocated_start - allocated_end).0;
                        (elapsed_index, elapsed_run, index_size_bytes)
                    };

                    reporter.report_parallel(
                        threads,
                        elapsed_index,
                        elapsed_run,
                        index_size_bytes,
                    )?;
                }
                ExperimentMode::Batch => {
                    let (elapsed_index, elapsed_run, index_size_bytes) = {
                        // we need to run into a block (with some code duplication) in order to satisfy runtime constraints
                        // on the RefCell which is borrowed mutable by the algorithm

                        let mut algorithm = experiment.algorithm.borrow_mut();
                        info!("Building index");
                        let start = Instant::now();
                        algorithm.index(&dataset_intervals);
                        let end = Instant::now();
                        let elapsed_index = (end - start).as_millis() as i64; // truncation happens here, but only on extremely long runs
                        info!("Index built in {:?}", end - start);

                        info!("Running queries [batch]");
                        let start = Instant::now();
                        let matches = algorithm.run_batch(&queryset_queries);
                        let end = Instant::now();
                        let elapsed_run = (end - start).as_millis() as i64; // truncation happens here, but only on extremely long runs

                        info!(
                            "time for index {}ms, time for query {}ms, output size {}",
                            elapsed_index, elapsed_run, matches
                        );
                        // Clear up the index to free resources, and measure the space it took up
                        let allocated_start = get_allocated();
                        algorithm.clear();
                        let allocated_end = get_allocated();
                        assert!(allocated_start.0 > allocated_end.0);
                        info!("Size of the index was {}", allocated_start - allocated_end);
                        let index_size_bytes = (allocated_start - allocated_end).0;
                        (elapsed_index, elapsed_run, index_size_bytes)
                    };

                    reporter.report_batch(elapsed_index, elapsed_run, index_size_bytes)?;
                }
                ExperimentMode::Focus { samples } => {
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
                ExperimentMode::Insertion { batch } => {
                    let results = {
                        let mut algorithm = experiment.algorithm.borrow_mut();
                        algorithm.clear();
                        let results = algorithm.run_inserts(&dataset_intervals, batch);
                        algorithm.clear();
                        results
                    };
                    reporter.report_insert(batch, results)?;
                }
            }

            Ok(())
        })?;
    }

    Ok(())
}

// Implementation below taken from https://github.com/rust-analyzer/rust-analyzer/blob/b988c6f84e06bdc5562c70f28586b9eeaae3a39c/crates/profile/src/memory_usage.rs
#[derive(Default, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Bytes(usize);

impl Bytes {
    pub fn megabytes(self) -> usize {
        self.0 / 1024 / 1024
    }
}

impl std::fmt::Display for Bytes {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let bytes = self.0;
        let mut value = bytes;
        let mut suffix = "b";
        if value > 4096 {
            value /= 1024;
            suffix = "kb";
            if value > 4096 {
                value /= 1024;
                suffix = "mb";
            }
        }
        f.pad(&format!("{}{}", value, suffix))
    }
}

impl std::ops::AddAssign<usize> for Bytes {
    fn add_assign(&mut self, x: usize) {
        self.0 += x;
    }
}

impl std::ops::Sub for Bytes {
    type Output = Bytes;
    fn sub(self, rhs: Bytes) -> Bytes {
        Bytes(self.0 - rhs.0)
    }
}
