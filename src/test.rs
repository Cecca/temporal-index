use paste::paste;

use crate::btree::BTree;
use crate::dataset::*;
use crate::ebi::EBIIndex;
use crate::grid::Grid;
use crate::grid3d::Grid3D;
use crate::interval_tree::IntervalTree;
use crate::naive::LinearScan;
use crate::period_index::PeriodIndex;
use crate::types::*;

fn run_test_same_result(data: &Vec<Interval>, queries: &Vec<Query>, mut algo: Box<dyn Algorithm>) {
    let mut linear_scan = LinearScan::new();
    linear_scan.index(&data);
    let baseline = linear_scan.run(&queries);

    algo.index(&data);
    let algo_results = algo.run(&queries);

    for (idx, (ls_ans, pi_ans)) in algo_results
        .into_iter()
        .zip(baseline.into_iter())
        .enumerate()
    {
        assert_eq!(
            ls_ans.intervals(),
            pi_ans.intervals(),
            "query is {:?}",
            queries[idx]
        );
    }
}

macro_rules! same_result {
    ($($name:ident: $value:expr,)*) => {
    $(
        paste! {
            #[test]
            fn [<$name _btree>]() {
                let (data, queries) = &$value;
                run_test_same_result(data, queries, Box::new(BTree::new()));
            }

            #[test]
            fn [<$name _period_index>]() {
                let (data, queries) = &$value;
                run_test_same_result(data, queries, Box::new(PeriodIndex::new(128, 4).unwrap()));
            }

            #[test]
            fn [<$name _grid>]() {
                let (data, queries) = &$value;
                run_test_same_result(data, queries, Box::new(Grid::new(128)));
            }

            #[test]
            fn [<$name _grid3d>]() {
                let (data, queries) = &$value;
                run_test_same_result(data, queries, Box::new(Grid3D::new(128)));
            }

            #[test]
            fn [<$name _interval_tree>]() {
                let (data, queries) = &$value;
                run_test_same_result(data, queries, Box::new(IntervalTree::new()));
            }

            #[test]
            fn [<$name _ebi>]() {
                let (data, queries) = &$value;
                run_test_same_result(data, queries, Box::new(EBIIndex::default()));
            }
        }
    )*
    }
}

same_result! {
    rand1: (
        RandomDatasetZipfAndUniform::new(12351, 1000000, 1.0, 1000).get(),
        RandomQueriesZipfAndUniform::new(123415, 5, 1.0, 1000, 0.4).get()
    ),
}
