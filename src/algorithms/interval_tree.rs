use crate::types::*;

pub struct IntervalTree {
    n: usize,
    root: Option<Box<Node>>,
}

impl IntervalTree {
    pub fn new() -> Self {
        Self { n: 0, root: None }
    }

    fn query<F: FnMut(Interval)>(&self, query: Interval, action: &mut F) -> u32 {
        let mut cnt = 0u32;
        let mut node = self.root.as_ref();
        while node.is_some() {
            let node_ref = node.unwrap();
            // We use <= because the end point is not part of the interval,
            // hence the current node is not the fork node and we have to
            // descend further
            if query.end <= node_ref.middle {
                cnt += node_ref.overlapping_lower(query.end, action);
                node = node_ref.left.as_ref();
            } else if node_ref.middle < query.start {
                cnt += node_ref.overlapping_upper(query.start, action);
                node = node_ref.right.as_ref();
            } else {
                debug_assert!(query.contains(node_ref.middle));
                // we are at the fork node, iterate through all the values,
                // which are all overlapping the query
                node_ref.node_intervals().for_each(|interval| {
                    debug_assert!(query.overlaps(&interval));
                    cnt += 1;
                    action(interval);
                });
                break;
            }
        }
        if node.is_none() {
            // We are done
            return cnt;
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
                cnt += node_ref.overlapping_upper(query.start, action);
                cursor = node_ref.right.as_ref();
            } else {
                // We should steer left
                node_ref.node_intervals().for_each(|interval| {
                    debug_assert!(query.overlaps(&interval));
                    cnt += 1;
                    action(interval);
                });
                node_ref.right.as_ref().map(|child| {
                    child.subtree_intervals(&mut |interval| {
                        cnt += 1;
                        action(interval);
                    })
                });
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
                cnt += node_ref.overlapping_lower(query.end, action);
                cursor = node_ref.left.as_ref();
            } else {
                node_ref.node_intervals().for_each(|interval| {
                    debug_assert!(query.overlaps(&interval));
                    cnt += 1;
                    action(interval);
                });
                node_ref.left.as_ref().map(|child| {
                    child.subtree_intervals(&mut |interval| {
                        cnt += 1;
                        action(interval);
                    })
                });
                cursor = node_ref.right.as_ref();
            }
        }

        cnt
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

    fn size(&self) -> usize {
        if let Some(root) = self.root.as_ref() {
            root.size()
        } else {
            0
        }
    }

    fn parameters(&self) -> String {
        String::new()
    }
    fn version(&self) -> u8 {
        5
    }
    fn index(&mut self, dataset: &[Interval]) {
        let mut intervals: Vec<Interval> = dataset.iter().copied().collect();
        // Sort by middle point
        intervals.sort_by_key(|interval| interval.middle());
        self.root = Some(Box::new(Node::new(&intervals)));
        self.n = intervals.len();
    }

    fn insert(&mut self, interval: Interval) {
        if let Some(root) = self.root.as_mut() {
            root.insert(interval);
        } else {
            let root = Box::new(Node::new(&vec![interval]));
            self.root.replace(root);
        }
    }

    fn remove(&mut self, interval: Interval) {
        if let Some(root) = self.root.as_mut() {
            root.remove(interval);
        }
    }

    fn query(&self, query: &Query, answers: &mut QueryAnswerBuilder) {
        if let Some(range) = query.range {
            if let Some(duration_range) = query.duration {
                let cnt = self.query(range, &mut |i| {
                    debug_assert!(
                        i.overlaps(&range),
                        "interval: {:?}, query range: {:?}",
                        i,
                        range
                    );
                    if duration_range.contains(&i) {
                        answers.push(i);
                    }
                });
                answers.inc_examined(cnt);
            } else {
                let cnt = self.query(range, &mut |i| {
                    debug_assert!(
                        i.overlaps(&range),
                        "interval: {:?}, query range: {:?}",
                        i,
                        range
                    );
                    answers.push(i);
                });
                answers.inc_examined(cnt);
            }
        } else {
            if let Some(duration_range) = query.duration {
                let mut cnt = 0;
                self.root.as_ref().unwrap().subtree_intervals(&mut |i| {
                    cnt += 1;
                    if duration_range.contains(&i) {
                        answers.push(i);
                    }
                });
                answers.inc_examined(cnt);
            } else {
                let mut cnt = 0;
                self.root.as_ref().unwrap().subtree_intervals(&mut |i| {
                    cnt += 1;
                    answers.push(i);
                });
                answers.inc_examined(cnt);
            }
        }
    }

    fn par_query(&self, query: &Query, answer: &mut QueryAnswerBuilder) {
        unimplemented!()
    }

    fn clear(&mut self) {
        let root = self.root.take();
        // This drops all the children as well, because they are owned by the root
        drop(root);
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

    fn size(&self) -> usize {
        let mut s = self.upper.len();
        if let Some(l) = self.left.as_ref() {
            s += l.size()
        }
        if let Some(r) = self.right.as_ref() {
            s += r.size();
        }
        s
    }

    fn insert(&mut self, interval: Interval) {
        if interval.end <= self.middle {
            // insert left
            if let Some(left) = self.left.as_mut() {
                left.insert(interval);
            } else {
                self.left.replace(Box::new(Self::new(&vec![interval])));
            }
        } else if interval.start > self.middle {
            // insert right
            if let Some(right) = self.right.as_mut() {
                right.insert(interval);
            } else {
                self.right.replace(Box::new(Self::new(&vec![interval])));
            }
        } else {
            // insert into upper and lower of this
            self.upper.push(interval);
            self.lower.push(interval);
            // TODO: insert directly in order
            self.lower.sort_by_key(|i| i.start);
            self.upper.sort_by_key(|i| -(i.end as i32));
        }
    }

    fn remove(&mut self, interval: Interval) {
        if interval.end <= self.middle {
            // remove from left, if any
            if let Some(left) = self.left.as_mut() {
                left.remove(interval);
            }
        } else if interval.start > self.middle {
            // remove from  right, if any
            if let Some(right) = self.right.as_mut() {
                right.remove(interval);
            }
        } else {
            // remove from self
            self.upper.retain(|i| i != &interval);
            self.lower.retain(|i| i != &interval);
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
    fn overlapping_upper<F: FnMut(Interval)>(&self, query_start: Time, action: &mut F) -> u32 {
        let mut cnt = 0;
        self.upper
            .iter()
            .take_while(|interval| query_start < interval.end)
            .for_each(|interval| {
                cnt += 1;
                action(*interval);
            });
        cnt
    }

    /// Finds the intervals in the lower list such that the given point is
    /// strictly greater than the start point of the intervals
    fn overlapping_lower<F: FnMut(Interval)>(&self, query_end: Time, action: &mut F) -> u32 {
        let mut cnt = 0;
        self.lower
            .iter()
            .take_while(|interval| interval.start < query_end)
            .for_each(|interval| {
                cnt += 1;
                action(*interval);
            });
        cnt
    }
}
