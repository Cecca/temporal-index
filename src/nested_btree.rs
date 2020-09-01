use crate::types::*;
use deepsize::DeepSizeOf;
use std::collections::BTreeMap;

pub struct NestedBTree {
    /// A map from duration to trees. The inner tree map starting times
    /// of intervals with that duration with the count of intervals starting there
    inner: BTreeMap<Time, BTreeMap<Time, usize>>,
}

impl Default for NestedBTree {
    fn default() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }
}

impl deepsize::DeepSizeOf for NestedBTree {
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        // I actually don't know the overhead of the BTReeMap
        self.inner.iter().fold(0, |sum, (key, val)| {
            let m_size = val.iter().fold(0, |sum, (key, val)| {
                sum + key.deep_size_of_children(context) + val.deep_size_of_children(context)
            });
            sum + key.deep_size_of_children(context) + m_size
        })
    }
}

impl std::fmt::Debug for NestedBTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "NestedBTree with {} indexed intervals, {} distinct durations",
            self.n_intervals(),
            self.n_durations()
        )
    }
}

impl NestedBTree {
    fn n_durations(&self) -> usize {
        self.inner.len()
    }

    fn n_intervals(&self) -> usize {
        self.inner
            .iter()
            .map(|(_, m)| m.values().sum::<usize>())
            .sum()
    }
}

impl Algorithm for NestedBTree {
    fn name(&self) -> String {
        String::from("NestedBTree")
    }
    fn parameters(&self) -> String {
        String::new()
    }
    fn version(&self) -> u8 {
        3
    }
    fn index(&mut self, dataset: &[Interval]) {
        self.clear();
        let mut pl = progress_logger::ProgressLogger::builder()
            .with_items_name("intervals")
            .with_expected_updates(dataset.len() as u64)
            .start();
        for interval in dataset {
            self.inner
                .entry(interval.duration())
                .or_insert_with(|| BTreeMap::new())
                .entry(interval.start)
                .and_modify(|c| *c += 1)
                .or_insert(1);
            pl.update_light(1u64);
        }
        pl.stop();
        let size = self.deep_size_of();
        info!(
            "Allocated for index: {} bytes ({} Mb)",
            size,
            size / (1024 * 1024)
        );
    }

    fn query(&self, query: &Query, answers: &mut QueryAnswerBuilder) {
        match (query.range, query.duration) {
            (Some(range), Some(duration)) => {
                for (&d, tree) in self.inner.range(duration.min..=duration.max) {
                    let r_start = if range.start < d {
                        0
                    } else {
                        range.start - d + 1
                    };
                    for (&start, &count) in tree.range(r_start..range.end) {
                        let interval = Interval::new(start, d);
                        debug_assert!(duration.contains(&interval));
                        debug_assert!(
                            range.overlaps(&interval),
                            "range: {:?} interval: {:?}, duration {} (start {})",
                            range,
                            interval,
                            d,
                            r_start
                        );
                        for _ in 0..count {
                            answers.push(interval);
                        }
                    }
                }
            }
            (Some(range), None) => {
                for (&d, tree) in self.inner.iter() {
                    let start = if range.start < d {
                        0
                    } else {
                        range.start - d + 1
                    };
                    for (&start, &count) in tree.range(start..range.end) {
                        let interval = Interval::new(start, d);
                        debug_assert!(range.overlaps(&interval));
                        for _ in 0..count {
                            answers.push(interval);
                        }
                    }
                }
            }
            (None, Some(duration)) => {
                for (&d, tree) in self.inner.range(duration.min..=duration.max) {
                    debug_assert!(duration.min <= d && d <= duration.max);
                    tree.iter().for_each(|(&start, &count)| {
                        let interval = Interval::new(start, d);
                        debug_assert!(duration.contains(&interval));
                        for _ in 0..count {
                            answers.push(interval);
                        }
                    });
                }
            }
            (None, None) => todo!(),
        }
    }

    fn clear(&mut self) {
        self.inner.clear();
    }
}
