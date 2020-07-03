use crate::types::*;
use deepsize::DeepSizeOf;

#[derive(DeepSizeOf)]
pub struct IntervalTree {
    n: usize,
    root: Option<Box<Node>>,
}

impl IntervalTree {
    pub fn new() -> Self {
        Self { n: 0, root: None }
    }

    fn query<F: FnMut(Interval)>(&self, query: Interval, action: &mut F) {
        let mut node = self.root.as_ref();
        while node.is_some() {
            let node_ref = node.unwrap();
            // We use <= because the end point is not part of the interval,
            // hence the current node is not the fork node and we have to
            // descend further
            if query.end <= node_ref.middle {
                node_ref.overlapping_upper(query.start, action);
                node = node_ref.left.as_ref();
            } else if node_ref.middle < query.start {
                node_ref.overlapping_lower(query.end, action);
                node = node_ref.right.as_ref();
            } else {
                debug_assert!(query.contains(node_ref.middle));
                // we are at the fork node, iterate through all the values,
                // which are all overlapping the query
                node_ref.node_intervals().for_each(|interval| {
                    debug_assert!(query.overlaps(&interval));
                    action(interval);
                });
                break;
            }
        }
        if node.is_none() {
            // We are done
            return;
        }
        let left = node.expect("None fork node").left.as_ref();
        let right = node.expect("None fork node").right.as_ref();

        // Descend into the left subtree. From now on, we go right if the
        // middle point of the node is less than the start of the query,
        // and left otherwise. For nodes with middle point less than the start
        // of the query we scan the upper list. Otherwise we report all the
        // intervals of the node and all the intervals of the right subtree.
        let mut cursor = left;
        while cursor.is_some() {
            let node_ref = cursor.unwrap();
            if node_ref.middle < query.start {
                // We should steer right
                node_ref.overlapping_upper(query.start, action);
                cursor = node_ref.right.as_ref();
            } else {
                // We should steer left
                node_ref.node_intervals().for_each(|interval| {
                    debug_assert!(query.overlaps(&interval));
                    action(interval);
                });
                node_ref
                    .right
                    .as_ref()
                    .map(|child| child.subtree_intervals(action));
                cursor = node_ref.left.as_ref();
            }
        }

        // Descend into the right subtree of the fork node. We go left if the middle
        // point of the node is greater than or _equal_ (because the end
        // is not part of an interval), otherwise we go right.
        // In the first case we scan the lower list, in the second we report
        // all the intervals in the node and in the left subtree.
        let mut cursor = right;
        while cursor.is_some() {
            let node_ref = cursor.unwrap();
            if query.end <= node_ref.middle {
                node_ref.overlapping_lower(query.end, action);
                cursor = node_ref.left.as_ref();
            } else {
                node_ref.node_intervals().for_each(|interval| {
                    debug_assert!(query.overlaps(&interval));
                    action(interval);
                });
                node_ref
                    .left
                    .as_ref()
                    .map(|child| child.subtree_intervals(action));
                cursor = node_ref.right.as_ref();
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
        let size = self.deep_size_of();
        info!("Allocated for index: {} bytes ({} Mb)", size, size / (1024*1024));
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
                self.root.as_ref().unwrap().subtree_intervals(&mut |i| {
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

#[derive(DeepSizeOf)]
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
            // The end point is not part of the interval, hence we have to
            // put the interval in the left subtree it if is equal to the
            // middle point
            if interval.end <= middle {
                to_left.push(interval);
            } else if interval.start > middle {
                to_right.push(interval)
            } else {
                debug_assert!(
                    interval.contains(middle),
                    "{:?} middle: {}",
                    interval,
                    middle
                );
                upper.push(interval);
                lower.push(interval);
            }
        }
        lower.sort_by_key(|i| i.start);
        upper.sort_by_key(|i| -(i.end as i32));

        let left = if to_left.is_empty() {
            None
        } else {
            trace!("insert left {} intervals", to_left.len());
            Some(Box::new(Self::new(&to_left)))
        };

        let right = if to_right.is_empty() {
            None
        } else {
            trace!("insert right {} intervals", to_right.len());
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

    fn subtree_intervals<F: FnMut(Interval)>(&self, action: &mut F) {
        self.traverse(&mut |node: &Node| {
            node.lower.iter().for_each(|i| action(*i));
        })
    }

    fn node_intervals(&self) -> impl Iterator<Item = Interval> + '_ {
        self.upper.iter().copied()
    }

    /// Finds the intervals in the upper list such that the given
    /// point is less than the end point of the intervals
    fn overlapping_upper<F: FnMut(Interval)>(&self, point: Time, action: &mut F) {
        self.upper
            .iter()
            .take_while(|interval| point < interval.end)
            .for_each(|interval| {
                if interval.contains(point) {
                    action(*interval);
                }
            });
    }

    /// Finds the intervals in the lower list such that the given point is
    /// strictly greater than the start point of the intervals
    fn overlapping_lower<F: FnMut(Interval)>(&self, point: Time, action: &mut F) {
        self.lower
            .iter()
            .take_while(|interval| interval.start < point)
            .for_each(|interval| {
                if interval.contains(point) {
                    action(*interval);
                } 
            });
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
