use crate::types::*;

pub struct IntervalTree {
    n: usize,
    root: Option<Box<Node>>,
}

impl IntervalTree {
    fn new() -> Self {
        Self { n: 0, root: None }
    }

    fn query<F: FnMut(Interval)>(&self, query: Interval, action: &mut F) {
        let mut node = self.root.as_ref();
        while node.is_some() {
            println!("looking into {:?}, {:?}", node, query);
            let node_ref = node.unwrap();
            if query.end < node_ref.middle {
                node_ref.overlapping_upper(query.start, action);
                println!("going left");
                node = node_ref.left.as_ref();
            } else if node_ref.middle < query.start {
                // TODO use binary search here
                node_ref.lower.iter().for_each(|interval| {
                    if query.overlaps(interval) {
                        action(*interval);
                    }
                });
                println!("going right");
                node = node_ref.right.as_ref();
            } else {
                println!("fork node for {:?} ({:?})", node, query);
                debug_assert!(query.contains(node_ref.middle));
                // we are at the fork node, iterate through all the values,
                // which are all overlapping the query
                node_ref.lower.iter().for_each(|interval| {
                    debug_assert!(query.overlaps(interval));
                    action(*interval);
                });
                break;
            }
        }
        if node.is_none() {
            // We are done
            return;
        }
        let mut left = node.expect("None fork node").left.as_ref();
        let mut right = node.expect("None fork node").right.as_ref();

        // Explore to the left of the fork node
        while left.is_some() {
            let left_ref = left.unwrap();
            if query.end < left_ref.middle {
                // TODO use binary search here
                left_ref.upper.iter().for_each(|interval| {
                    if query.overlaps(interval) {
                        action(*interval);
                    }
                });
                left = left_ref.right.as_ref();
            } else {
                left_ref.upper.iter().for_each(|interval| {
                    if query.overlaps(interval) {
                        action(*interval);
                    }
                });
                if let Some(right) = left_ref.right.as_ref() {
                    right.for_each_interval(action);
                }
                left = left_ref.left.as_ref();
            }
        }

        // Explore to the right of the fork node
        while right.is_some() {
            let right_ref = right.unwrap();
            if right_ref.middle < query.end {
                // TODO use binary search here
                right_ref.lower.iter().for_each(|interval| {
                    if query.overlaps(interval) {
                        action(*interval);
                    }
                });
                right = right_ref.left.as_ref();
            } else {
                right_ref.upper.iter().for_each(|interval| {
                    if query.overlaps(interval) {
                        action(*interval);
                    }
                });
                if let Some(left) = right_ref.left.as_ref() {
                    left.for_each_interval(action);
                }
                right = right_ref.right.as_ref();
            }
        }
    }
}

impl std::fmt::Debug for IntervalTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "interval-tree")
    }
}

impl Algorithm for IntervalTree {
    fn name(&self) -> String {
        String::from("interval-tree")
    }
    fn parameters(&self) -> String {
        String::new()
    }
    fn version(&self) -> u8 {
        1
    }
    fn index(&mut self, dataset: &[Interval]) {
        let mut intervals: Vec<Interval> = dataset.iter().copied().collect();
        // Sort by middle point
        intervals.sort_by_key(|interval| interval.middle());
        self.root = Some(Box::new(Node::new(&intervals)));
        self.n = intervals.len();
    }
    fn run(&self, queries: &[Query]) -> Vec<QueryAnswer> {
        let mut answers = Vec::with_capacity(queries.len());
        for query in queries {
            let mut ans_builder = QueryAnswer::builder(self.n);
            if let Some(range) = query.range {
                self.query(range, &mut |i| {
                    debug_assert!(
                        i.overlaps(&range),
                        "interval: {:?}, query range: {:?}",
                        i,
                        range
                    );
                    // TODO: do the unwrapping once and for all
                    let matches_duration = query
                        .duration
                        .as_ref()
                        .map(|d| d.contains(&i))
                        .unwrap_or(true);
                    if matches_duration {
                        ans_builder.push(i);
                    }
                })
            } else {
                self.root.as_ref().unwrap().for_each_interval(&mut |i| {
                    let matches_duration = query
                        .duration
                        .as_ref()
                        .map(|d| d.contains(&i))
                        .unwrap_or(true);
                    let overlaps = query
                        .range
                        .as_ref()
                        .map(|range| range.overlaps(&i))
                        .unwrap_or(true);
                    if matches_duration && overlaps {
                        ans_builder.push(i);
                    }
                })
            }
            answers.push(ans_builder.finalize());
        }
        answers
    }
}

struct Node {
    pub middle: Time,
    pub upper: Vec<Interval>,
    pub lower: Vec<Interval>,
    pub left: Option<Box<Node>>,
    pub right: Option<Box<Node>>,
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "node with middle={}", self.middle)
    }
}

impl Node {
    fn new(intervals: &[Interval]) -> Self {
        // TODO: Get rid of allocations to save time
        let middle_interval = intervals[intervals.len() / 2];
        let middle = middle_interval.middle();
        let mut upper = Vec::new();
        let mut lower = Vec::new();
        let mut to_left = Vec::new();
        let mut to_right = Vec::new();
        for &interval in intervals.into_iter() {
            if interval.end < middle {
                to_left.push(interval);
            } else if interval.start > middle {
                to_right.push(interval)
            } else {
                debug_assert!(
                    interval.contains(middle) || middle == interval.end,
                    "{:?} middle: {}",
                    interval,
                    middle
                );
                upper.push(interval);
                lower.push(interval);
            }
        }
        lower.sort_by_key(|i| i.start);
        upper.sort_by_key(|i| i.end);

        let left = if to_left.is_empty() {
            None
        } else {
            println!("insert left {} intervals", to_left.len());
            debug_assert!(to_left.is_sorted_by_key(|i| i.middle()));
            Some(Box::new(Self::new(&to_left)))
        };

        let right = if to_right.is_empty() {
            None
        } else {
            println!("insert right {} intervals", to_right.len());
            debug_assert!(to_right.is_sorted_by_key(|i| i.middle()));
            Some(Box::new(Self::new(&to_right)))
        };

        Self {
            middle,
            upper,
            lower,
            left,
            right,
        }
    }

    fn traverse<F: FnMut(&Node)>(&self, action: &mut F) {
        action(self);
        if let Some(left) = &self.left {
            left.traverse(action);
        }
        if let Some(right) = &self.right {
            right.traverse(action)
        }
    }

    fn for_each_interval<F: FnMut(Interval)>(&self, action: &mut F) {
        self.traverse(&mut |node: &Node| {
            node.lower.iter().for_each(|i| action(*i));
        })
    }

    /// Finds the intervals in the upper list such that the given
    /// point is less than the end point of the intervals
    fn overlapping_upper<F: FnMut(Interval)>(&self, point: Time, action: &mut F) {
        self.upper.iter().for_each(|interval| {
            if interval.contains(point) {
                action(*interval);
            }
        });
        // let idx = match self.upper.binary_search_by_key(&point, |i| i.end) {
        //     Ok(mut idx) => {
        //         // position at the first matching element
        //         while idx > 0 && self.upper[idx-1].end == point {
        //             idx -= 1;
        //         }
        //         idx
        //     },
        //     Err(idx) => idx,
        // };
        // self.upper[idx..self.upper.len()].iter().for_each(|i| action(*i));
    }

    /// Finds the intervals in the lower list such that the given point is
    /// greater than the start point of the intervals
    fn overlapping_lower<F: FnMut(Interval)>(&self, point: Time, action: &mut F) {
        let idx = match self.lower.binary_search_by_key(&point, |i| i.start) {
            Ok(mut idx) => {
                // position after the last matching element
                while idx < self.lower.len() && self.lower[idx].start == point {
                    idx += 1;
                }
                idx
            }
            Err(idx) => idx,
        };
        self.lower[0..idx].iter().for_each(|i| action(*i));
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::dataset::*;
    use crate::naive::*;

    #[test]
    fn test_same_result_interval_tree_small() {
        let data = RandomDatasetZipfAndUniform::new(12351, 1000000, 1.0, 1000).get();
        let queries = vec![Query {
            range: Some(Interval::new(18, 4)),
            duration: Some(DurationRange::new(1, 1)),
        }];

        let mut linear_scan = LinearScan::new();
        linear_scan.index(&data);
        let ls_result = linear_scan.run(&queries);

        let mut period_index = IntervalTree::new();
        period_index.index(&data);
        let pi_result = period_index.run(&queries);

        for (ls_ans, pi_ans) in ls_result.into_iter().zip(pi_result.into_iter()) {
            assert_eq!(ls_ans.intervals(), pi_ans.intervals());
        }
    }

    #[test]
    fn test_same_result_interval_tree() {
        let data = RandomDatasetZipfAndUniform::new(123, 10, 1.0, 1000).get();
        let queries = RandomQueriesZipfAndUniform::new(1734, 10, 1.0, 1000, 0.5).get();

        let mut linear_scan = LinearScan::new();
        linear_scan.index(&data);
        let ls_result = linear_scan.run(&queries);

        let mut period_index = IntervalTree::new();
        period_index.index(&data);
        let pi_result = period_index.run(&queries);

        for (ls_ans, pi_ans) in ls_result.into_iter().zip(pi_result.into_iter()) {
            assert_eq!(ls_ans.intervals(), pi_ans.intervals());
        }
    }

    #[test]
    fn test_same_result_2_interval_tree() {
        let data = RandomDatasetZipfAndUniform::new(12351, 1000000, 1.0, 1000).get();
        let queries = RandomQueriesZipfAndUniform::new(123415, 5, 1.0, 1000, 0.4).get();

        let mut linear_scan = LinearScan::new();
        linear_scan.index(&data);
        let ls_result = linear_scan.run(&queries);

        let mut period_index = IntervalTree::new();
        period_index.index(&data);
        let pi_result = period_index.run(&queries);

        for (idx, (ls_ans, pi_ans)) in ls_result.into_iter().zip(pi_result.into_iter()).enumerate()
        {
            assert_eq!(
                ls_ans.intervals(),
                pi_ans.intervals(),
                "query is {:?}",
                queries[idx]
            );
        }
    }
}
