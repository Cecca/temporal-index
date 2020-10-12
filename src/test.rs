use paste::paste;

use crate::algorithms::*;
use crate::dataset::*;
use crate::types::*;
use itertools::Itertools;
use std::collections::BTreeSet;
use std::iter::FromIterator;

fn run_test_same_result(data: &Vec<Interval>, queries: &Vec<Query>, mut algo: Box<dyn Algorithm>) {
    let _ = pretty_env_logger::formatted_builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Debug)
        .try_init();
    let mut linear_scan = LinearScan::new();
    linear_scan.index(&data);
    let baseline = linear_scan.run_recording(&queries);

    algo.index(&data);
    let algo_results = algo.run_recording(&queries);

    for (idx, (ls_ans, algo_ans)) in algo_results
        .into_iter()
        .zip(baseline.into_iter())
        .enumerate()
    {
        let mut expected = ls_ans.intervals();
        let mut actual = algo_ans.intervals();
        expected.sort_unstable();
        actual.sort_unstable();
        assert!(
            expected == actual,
            "query {} is {:?}, {} false negatives, {} false positives:\nfalse negatives:\n{}",
            idx,
            queries[idx],
            BTreeSet::from_iter(actual.iter())
                .difference(&BTreeSet::from_iter(expected.iter()))
                .count(),
            BTreeSet::from_iter(expected.iter())
                .difference(&BTreeSet::from_iter(actual.iter()))
                .count(),
            Vec::from_iter(
                BTreeSet::from_iter(actual.iter())
                    .difference(&BTreeSet::from_iter(expected.iter()))
            )
            .iter()
            .map(|interval| format!("{:?} [d={}]", interval, interval.duration()))
            .join("\n")
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
            fn [<$name _period_index_star>]() {
                let (data, queries) = &$value;
                run_test_same_result(data, queries, Box::new(PeriodIndexStar::new(128, 4).unwrap()));
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
            fn [<$name _nested_btree>]() {
                let (data, queries) = &$value;
                run_test_same_result(data, queries, Box::new(NestedBTree::default()));
            }

            #[test]
            fn [<$name _nested_vecs>]() {
                let (data, queries) = &$value;
                run_test_same_result(data, queries, Box::new(NestedVecs::default()));
            }

            #[test]
            fn [<$name _period_index_plusplus>]() {
                let (data, queries) = &$value;
                run_test_same_result(data, queries, Box::new(PeriodIndexPlusPlus::new(5)));
            }

            #[test]
            fn [<$name _period_index_plusplus_ps_10>]() {
                let (data, queries) = &$value;
                run_test_same_result(data, queries, Box::new(PeriodIndexPlusPlus::new(10)));
            }

            // #[test]
            // fn [<$name _period_index_plusplus_manybucks>]() {
            //     let (data, queries) = &$value;
            //     run_test_same_result(data, queries, Box::new(PeriodIndexPlusPlus::new(100000)));
            // }

            // #[test]
            // fn [<$name _ebi>]() {
            //     let (data, queries) = &$value;
            //     run_test_same_result(data, queries, Box::new(EBIIndex::default()));
            // }
        }
    )*
    }
}

same_result! {
    rand1: (
        RandomDatasetZipfAndUniform::new(12351, 1000000, 1.0, 1000).get(),
        RandomQueriesZipfAndUniform::new(123415, 5, 1.0, 1000, 0.4).get()
    ),
    rand_uniform: (
        RandomDataset::new(12351, 1000, TimeDistribution::Uniform{low: 1, high: 10000}, TimeDistribution::Uniform{low: 10, high: 1000}).get(),
        RandomQueryset::new(12351, 100,
                            Some((TimeDistribution::Uniform{low: 1, high: 10000}, TimeDistribution::Uniform{low: 10, high: 10000})),
                            Some((TimeDistribution::Uniform{low: 1, high: 100}, TimeDistribution::Uniform{low: 10, high: 1000})),
                            ).get(),
    ),
    rand_uniform_duration_only: (
        RandomDataset::new(12351, 1000, TimeDistribution::Uniform{low: 1, high: 10000}, TimeDistribution::Uniform{low: 10, high: 1000}).get(),
        RandomQueryset::new(12351, 100,
                            None,
                            Some((TimeDistribution::Uniform{low: 1, high: 100}, TimeDistribution::Uniform{low: 10, high: 1000})),
                            ).get(),
    ),
    rand_uniform_overlap_only: (
        RandomDataset::new(12351, 1000, TimeDistribution::Uniform{low: 1, high: 10000}, TimeDistribution::Uniform{low: 10, high: 1000}).get(),
        RandomQueryset::new(12351, 100,
                            Some((TimeDistribution::Uniform{low: 1, high: 10000}, TimeDistribution::Uniform{low: 10, high: 10000})),
                            None
                            ).get(),
    ),
    rand_clustered: (
        RandomDataset::new(12351, 1000, TimeDistribution::Clustered{n: 100, high: 10000, std_dev: 10}, TimeDistribution::Zipf{n: 10000, beta: 1.0}).get(),
        RandomQueryset::new(12351, 100,
                            Some((TimeDistribution::Uniform{low: 1, high: 10000}, TimeDistribution::Uniform{low: 10, high: 10000})),
                            Some((TimeDistribution::Uniform{low: 1, high: 100}, TimeDistribution::Uniform{low: 10, high: 1000})),
                            ).get(),
    ),

}
