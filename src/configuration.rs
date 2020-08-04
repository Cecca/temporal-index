use crate::dataset::*;
use crate::types::*;
use crate::period_index;
use crate::grid;
use crate::grid3d;
use crate::btree;
use crate::naive;
use crate::interval_tree;
use anyhow::{Context, Result};
use itertools::iproduct;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

#[derive(Debug, Clone)]
pub struct ExperimentConfiguration {
    pub dataset: Rc<dyn Dataset>,
    pub queries: Rc<dyn Queryset>,
    pub algorithm: Rc<RefCell<dyn Algorithm>>,
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
pub enum AlgorithmConfiguration {
    PeriodIndex {
        num_buckets: Vec<usize>,
        num_levels: Vec<u32>,
    },
    Grid {
        num_buckets: Vec<usize>,
    },
    Grid3D {
        num_buckets: Vec<usize>,
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
            Self::Grid { num_buckets } => {
                let iter = num_buckets.iter().map(|num_buckets| {
                    Rc::new(RefCell::new(grid::Grid::new(*num_buckets)))
                        as Rc<RefCell<dyn Algorithm>>
                });
                Box::new(iter)
            }
            Self::Grid3D { num_buckets } => {
                let iter = num_buckets.iter().map(|num_buckets| {
                    Rc::new(RefCell::new(grid3d::Grid3D::new(*num_buckets)))
                        as Rc<RefCell<dyn Algorithm>>
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
pub enum DataConfiguration {
    ZipfUniform {
        seed: Vec<u64>,
        n: Vec<usize>,
        exponent: Vec<f64>,
        max_start_time: Vec<u32>,
    },
    Random {
        seed: Vec<u64>,
        n: Vec<usize>,
        start_times: Vec<TimeDistribution>,
        durations: Vec<TimeDistribution>,
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
            Self::Random {
                seed,
                n,
                start_times,
                durations,
            } => {
                let iter = iproduct!(seed, n, start_times, durations).map(
                    |(seed, n, start_times, durations)| {
                        Rc::new(RandomDataset::new(*seed, *n, *start_times, *durations))
                            as Rc<dyn Dataset>
                    },
                );
                Box::new(iter)
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
pub enum QueryConfiguration {
    ZipfUniform {
        seed: Vec<u64>,
        n: Vec<usize>,
        exponent: Vec<f64>,
        max_start_time: Vec<u32>,
        max_duration_factor: Vec<f64>,
    },
    Random {
        seed: Vec<u64>,
        n: Vec<usize>,
        start_times: Vec<TimeDistribution>,
        durations: Vec<TimeDistribution>,
        start_durations: Vec<TimeDistribution>,
        duration_durations: Vec<TimeDistribution>,
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
            Self::Random {
                seed,
                n,
                start_times,
                durations,
                start_durations,
                duration_durations,
            } => {
                let iter = iproduct!(
                    seed,
                    n,
                    start_times,
                    durations,
                    start_durations,
                    duration_durations
                )
                .map(
                    |(seed, n, start_times, durations, start_durations, duration_durations)| {
                        Rc::new(RandomQueryset::new(
                            *seed,
                            *n,
                            *start_times,
                            *durations,
                            *start_durations,
                            *duration_durations,
                        )) as Rc<dyn Queryset>
                    },
                );
                Box::new(iter)
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Configuration {
    datasets: Vec<DataConfiguration>,
    queries: Vec<QueryConfiguration>,
    algorithms: Vec<AlgorithmConfiguration>,
}

impl Configuration {
    pub fn for_each<F: FnMut(ExperimentConfiguration) -> Result<()>>(
        self,
        mut action: F,
    ) -> Result<()> {
        for dataset in self.datasets.iter().flat_map(|d| d.datasets()) {
            info!("{:=<60}", "");
            info!(
                "{} (v{}) {}",
                dataset.name(),
                dataset.version(),
                dataset.parameters()
            );
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

    fn for_each_dataset<F: FnMut(Rc<dyn Dataset>) -> Result<()>>(
        &self,
        mut action: F,
    ) -> Result<()> {
        for dataset in self.datasets.iter().flat_map(|d| d.datasets()) {
            action(dataset)?;
        }
        Ok(())
    }

    #[allow(unused)]
    fn for_each_queryset<F: FnMut(Rc<dyn Queryset>) -> Result<()>>(
        &self,
        mut action: F,
    ) -> Result<()> {
        for queryset in self.queries.iter().flat_map(|q| q.queries()) {
            action(queryset)?;
        }
        Ok(())
    }

    pub fn print_histogram(&self, what: String) -> Result<()> {
        match what.as_ref() {
            "dataset-start-times" => {
                self.for_each_dataset(|dataset| {
                    let h = dataset
                        .get()
                        .iter()
                        .map(|interval| interval.start)
                        .histogram();
                    let name = dataset.name();
                    let version = dataset.version();
                    let parameters = dataset.parameters();
                    for (x, cnt) in h {
                        println!("{}, {}, {}, {}, {}", name, version, parameters, x, cnt);
                    }
                    Ok(())
                })?;
            }
            "dataset-end-times" => {
                self.for_each_dataset(|dataset| {
                    let h = dataset
                        .get()
                        .iter()
                        .map(|interval| interval.end)
                        .histogram();
                    let name = dataset.name();
                    let version = dataset.version();
                    let parameters = dataset.parameters();
                    for (x, cnt) in h {
                        println!("{}, {}, {}, {}, {}", name, version, parameters, x, cnt);
                    }
                    Ok(())
                })?;
            }
            "dataset-durations" => {
                self.for_each_dataset(|dataset| {
                    let h = dataset
                        .get()
                        .iter()
                        .map(|interval| interval.duration())
                        .histogram();
                    let name = dataset.name();
                    let version = dataset.version();
                    let parameters = dataset.parameters();
                    for (x, cnt) in h {
                        println!("{}, {}, {}, {}, {}", name, version, parameters, x, cnt);
                    }
                    Ok(())
                })?;
            }
            _ => anyhow::bail!("unknown what"),
        }
        Ok(())
    }
}

trait Histogram {
    fn histogram(self) -> BTreeMap<u32, u32>;
}

impl<I: IntoIterator<Item = u32>> Histogram for I {
    fn histogram(self) -> BTreeMap<u32, u32> {
        let mut hist = BTreeMap::new();
        for x in self {
            hist.entry(x).and_modify(|c| *c += 1).or_insert(1);
        }
        hist
    }
}
