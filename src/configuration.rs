use crate::algorithms::*;
use crate::dataset::*;
use crate::types::*;
use anyhow::{Context, Result};
use itertools::iproduct;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

#[derive(Debug, Clone)]
pub struct ExperimentConfiguration {
    pub mode: ExperimentMode,
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
    PeriodIndexStar {
        num_buckets: Vec<u32>,
        num_levels: Vec<u32>,
    },
    PeriodIndexPlusPlus {
        page_size: Vec<usize>,
    },
    RDIndex {
        dimension_order: Vec<DimensionOrder>,
        page_size: Vec<usize>,
    },
    GridFile {
        side_cells: Vec<usize>,
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
    RTree,
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
            Self::PeriodIndexPlusPlus { page_size } => {
                let iter = iproduct!(page_size).map(|ps| {
                    Rc::new(RefCell::new(PeriodIndexPlusPlus::new(*ps)))
                        as Rc<RefCell<dyn Algorithm>>
                });
                Box::new(iter)
            }
            Self::RDIndex {
                dimension_order,
                page_size,
            } => {
                let iter = iproduct!(dimension_order, page_size).map(|(order, ps)| {
                    Rc::new(RefCell::new(RDIndex::new(*order, *ps))) as Rc<RefCell<dyn Algorithm>>
                });
                Box::new(iter)
            }
            Self::PeriodIndexStar {
                num_buckets,
                num_levels,
            } => {
                let iter = iproduct!(num_buckets, num_levels).map(|(nb, nl)| {
                    Rc::new(RefCell::new(
                        period_index::PeriodIndexStar::new(*nb, *nl)
                            .expect("error in configured algorithm"),
                    )) as Rc<RefCell<dyn Algorithm>>
                });
                Box::new(iter)
            }
            Self::GridFile { side_cells } => {
                let iter = side_cells.iter().map(|side_cells| {
                    Rc::new(RefCell::new(GridFile::new(*side_cells))) as Rc<RefCell<dyn Algorithm>>
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
            Self::RTree => {
                let algo: Rc<RefCell<dyn Algorithm>> = Rc::new(RefCell::new(RTreeIndex::default()));
                Box::new(Some(algo).into_iter())
            }
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub enum DataConfiguration {
    ZipfUniform {
        seed: Vec<u64>,
        n: Vec<usize>,
        exponent: Vec<f64>,
        max_start_time: Vec<Time>,
    },
    Random {
        seed: Vec<u64>,
        n: Vec<usize>,
        start_times: Vec<TimeDistribution>,
        durations: Vec<TimeDistribution>,
    },
    Csv {
        path: std::path::PathBuf,
        start_column: usize,
        end_column: usize,
        separator: u8,
        has_header: bool,
    },
    Sorted {
        base: Box<DataConfiguration>,
    },
    Reiterated {
        base: Box<DataConfiguration>,
        copies: usize,
    },
    Flight,
    Webkit,
    Tourism,
    MimicIII,
}

impl DataConfiguration {
    fn datasets(&self) -> Box<dyn Iterator<Item = Rc<dyn Dataset>> + '_> {
        match self {
            Self::Flight => {
                let d = Rc::new(FlightDataset::from_upstream().unwrap()) as Rc<dyn Dataset>;
                Box::new(Some(d).into_iter())
            }
            Self::Webkit => {
                let d = Rc::new(WebkitDataset::from_upstream().unwrap()) as Rc<dyn Dataset>;
                Box::new(Some(d).into_iter())
            }
            Self::Tourism => {
                let d = Rc::new(TourismDataset::from_upstream().unwrap()) as Rc<dyn Dataset>;
                Box::new(Some(d).into_iter())
            }
            Self::MimicIII => {
                let d = Rc::new(MimicIIIDataset::from_upstream().unwrap()) as Rc<dyn Dataset>;
                Box::new(Some(d).into_iter())
            }
            Self::Sorted { base } => {
                let mut iter = base.datasets();
                let dataset = iter.next().expect("Missing dataset");
                assert!(
                    iter.next().is_none(),
                    "only a single dataset is supported here"
                );
                let sorted = SortedDataset::new(dataset).expect("error getting the dataset");
                Box::new(Some(Rc::new(sorted) as Rc<dyn Dataset>).into_iter())
            }
            Self::Reiterated { base, copies } => {
                let mut iter = base.datasets();
                let dataset = iter.next().expect("Missing dataset");
                assert!(
                    iter.next().is_none(),
                    "only a single dataset is supported here"
                );
                let reiterated =
                    ReiteratedDataset::new(dataset, *copies).expect("error getting the dataset");
                Box::new(Some(Rc::new(reiterated) as Rc<dyn Dataset>).into_iter())
            }
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
                        Rc::new(RandomDataset::new(
                            *seed,
                            *n,
                            start_times.clone(),
                            durations.clone(),
                        )) as Rc<dyn Dataset>
                    },
                );
                Box::new(iter)
            }
            Self::Csv {
                path,
                start_column,
                end_column,
                separator,
                has_header,
            } => {
                let dataset = Rc::new(CsvDataset::new(
                    path.clone(),
                    *start_column,
                    *end_column,
                    *separator,
                    *has_header,
                )) as Rc<dyn Dataset>;
                let iter = Some(dataset).into_iter();
                Box::new(iter)
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
pub enum GeneratorConfig {
    Free,
    Single(TimeDistribution),
}

impl GeneratorConfig {
    fn get(&self) -> Option<TimeDistribution> {
        match self {
            Self::Free => None,
            Self::Single(t) => Some(t.clone()),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub enum GeneratorPairConfig {
    Free,
    Pair(TimeDistribution, TimeDistribution),
}

impl GeneratorPairConfig {
    fn get(&self) -> Option<(TimeDistribution, TimeDistribution)> {
        match self {
            Self::Free => None,
            Self::Pair(t1, t2) => Some((t1.clone(), t2.clone())),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub enum QueryConfiguration {
    ZipfUniform {
        seed: Vec<u64>,
        n: Vec<usize>,
        exponent: Vec<f64>,
        max_start_time: Vec<Time>,
        max_duration_factor: Vec<f64>,
    },
    Random {
        seed: Vec<u64>,
        n: Vec<usize>,
        range: Vec<GeneratorPairConfig>,
        duration: Vec<GeneratorConfig>,
    },
    Mixed {
        seed: Vec<u64>,
        n: Vec<usize>,
        range: Vec<GeneratorPairConfig>,
        duration: Vec<GeneratorConfig>,
        frac_duration: f64,
        frac_range: f64,
        frac_range_duration: f64,
    },
    RandomCapped {
        seed: Vec<u64>,
        n: Vec<usize>,
        cap: Vec<Time>,
        range: Vec<GeneratorPairConfig>,
        duration: Vec<GeneratorConfig>,
    },
    Systematic {
        seed: Vec<u64>,
        n: Vec<usize>,
        base: DataConfiguration,
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
                range,
                duration,
            } => {
                let iter = iproduct!(seed, n, range, duration).flat_map(
                    |(seed, n, range, duration)| match (range.get(), duration.get()) {
                        (None, None) => None,
                        (range, duration) => {
                            Some(Rc::new(RandomQueryset::new(*seed, *n, range, duration))
                                as Rc<dyn Queryset>)
                        }
                    },
                );
                Box::new(iter)
            }
            Self::Mixed {
                seed,
                n,
                range,
                duration,
                frac_duration,
                frac_range,
                frac_range_duration,
            } => {
                let iter = iproduct!(seed, n, range, duration).flat_map(
                    move |(seed, n, range, duration)| match (range.get(), duration.get()) {
                        (None, None) => None,
                        (range, duration) => Some(Rc::new(MixedQueries::new(
                            *seed,
                            *n,
                            range,
                            duration,
                            *frac_duration,
                            *frac_range,
                            *frac_range_duration,
                        )) as Rc<dyn Queryset>),
                    },
                );
                Box::new(iter)
            }
            Self::RandomCapped {
                seed,
                n,
                cap,
                range,
                duration,
            } => {
                let iter = iproduct!(seed, n, cap, range, duration).flat_map(
                    |(seed, n, cap, range, duration)| match (range.get(), duration.get()) {
                        (None, None) => None,
                        (range, duration) => Some(Rc::new(RandomCappedQueryset::new(
                            *seed, *n, *cap, range, duration,
                        )) as Rc<dyn Queryset>),
                    },
                );
                Box::new(iter)
            }
            Self::Systematic { seed, n, base } => {
                let mut datasets = base.datasets();
                let base = datasets.next().expect("missing first dataset");

                assert!(
                    datasets.next().is_none(),
                    "only the first dataset is used in Systematic configuration"
                );
                let iter = iproduct!(seed, n).flat_map(move |(seed, n)| {
                    Some(Rc::new(SystematicQueryset::new(*seed, *n, &base)) as Rc<dyn Queryset>)
                });
                Box::new(iter)
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ExperimentMode {
    Focus { samples: u32 },
    FocusParallel { samples: u32 },
    Batch,
    Parallel,
    Insertion { batch: usize },
}

#[derive(Serialize, Deserialize)]
pub struct Configuration {
    mode: ExperimentMode,
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
                        mode: self.mode.clone(),
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

    pub fn dump(&self, what: String) -> Result<()> {
        match what.as_ref() {
            "dataset" => self.for_each_dataset(|dataset| {
                let intervals = dataset.get()?;
                let name = dataset.name();
                let version = dataset.version();
                let parameters = dataset.parameters();
                for interval in intervals {
                    println!(
                        "{}, {}, {}, {}, {}",
                        name, version, parameters, interval.start, interval.end
                    );
                }
                Ok(())
            }),
            "queries" => self.for_each_queryset(|queryset| {
                let queries = queryset.get();
                let name = queryset.name();
                let version = queryset.version();
                let parameters = queryset.parameters();
                let na_pair = String::from("NA, NA");
                for query in queries {
                    let r_str = query
                        .range
                        .map(|i| format!("{}, {}", i.start, i.end))
                        .unwrap_or(na_pair.clone());
                    let d_str = query
                        .duration
                        .map(|d| format!("{}, {}", d.min, d.max))
                        .unwrap_or(na_pair.clone());
                    println!(
                        "{}, {}, {}, {}, {}",
                        name, version, parameters, r_str, d_str
                    );
                }
                Ok(())
            }),
            _ => anyhow::bail!("Unknown parameter {}", what),
        }
    }

    pub fn print_histogram(&self, what: String) -> Result<()> {
        match what.as_ref() {
            "dataset-start-times" => {
                self.for_each_dataset(|dataset| {
                    let h = dataset
                        .get()?
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
                        .get()?
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
                        .get()?
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
            "queryset-interval-durations" => {
                self.for_each_queryset(|queryset| {
                    let h = queryset
                        .get()
                        .iter()
                        .map(|query| query.range.map(|r| r.duration()).unwrap_or(std::u64::MAX))
                        .histogram();
                    let name = queryset.name();
                    let version = queryset.version();
                    let parameters = queryset.parameters();
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
    fn histogram(self) -> BTreeMap<Time, u32>;
}

impl<I: IntoIterator<Item = Time>> Histogram for I {
    fn histogram(self) -> BTreeMap<Time, u32> {
        let mut hist = BTreeMap::new();
        for x in self {
            hist.entry(x).and_modify(|c| *c += 1).or_insert(1);
        }
        hist
    }
}
