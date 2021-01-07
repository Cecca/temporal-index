use deepsize::DeepSizeOf;
use progress_logger::ProgressLogger;

pub type Time = u64;

#[derive(Clone, Copy, Ord, PartialOrd, Eq, PartialEq, DeepSizeOf)]
pub struct Interval {
    pub start: Time,
    pub end: Time,
}

impl std::fmt::Debug for Interval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{} -{}> {})", self.start, self.duration(), self.end)
    }
}

impl Interval {
    pub fn new(start: Time, duration: Time) -> Self {
        Self {
            start,
            end: start + duration,
        }
    }

    pub fn duration(&self) -> Time {
        self.end - self.start
    }

    pub fn overlaps(&self, other: &Self) -> bool {
        self.start < other.end && other.start < self.end
        // !(self.start > other.end || other.start > self.end)
    }

    pub fn contains(&self, point: Time) -> bool {
        self.start <= point && point < self.end
    }

    #[allow(dead_code)]
    pub fn contains_interval(&self, other: &Self) -> bool {
        self.start <= other.start && other.end <= self.end
    }

    pub fn middle(&self) -> Time {
        (self.end + self.start) / 2
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, DeepSizeOf)]
pub struct DurationRange {
    pub min: Time,
    pub max: Time,
}

impl DurationRange {
    pub fn new(min: Time, max: Time) -> Self {
        assert!(min <= max);
        Self { min, max }
    }

    pub fn contains(&self, interval: &Interval) -> bool {
        let d = interval.duration();
        self.min <= d && d <= self.max
    }

    pub fn contains_duration(&self, other: Self) -> bool {
        self.min <= other.min && other.max <= self.max
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, DeepSizeOf)]
pub struct Query {
    pub range: Option<Interval>,
    pub duration: Option<DurationRange>,
}

#[derive(Debug)]
pub struct QueryAnswer {
    #[cfg(test)]
    intervals: Vec<Interval>,
    examined: u32,
    n_matches: u32,
}

impl QueryAnswer {
    pub fn num_matches(&self) -> u32 {
        self.n_matches
    }

    pub fn num_examined(&self) -> u32 {
        self.examined
    }

    pub fn builder() -> QueryAnswerBuilder {
        QueryAnswerBuilder {
            #[cfg(test)]
            intervals: Vec::new(),
            examined: 0,
            n_matches: 0,
        }
    }

    #[cfg(test)]
    pub fn intervals(&self) -> Vec<Interval> {
        let mut res = self.intervals.clone();
        res.sort();
        res
    }
}

pub struct QueryAnswerBuilder {
    #[cfg(test)]
    intervals: Vec<Interval>,
    examined: u32,
    n_matches: u32,
}

impl QueryAnswerBuilder {
    #[allow(dead_code)]
    pub fn record_intervals(self) -> Self {
        Self {
            #[cfg(test)]
            intervals: Vec::new(),
            examined: 0,
            n_matches: 0,
        }
    }

    #[inline]
    pub fn inc_examined(&mut self, cnt: u32) {
        self.examined += cnt;
    }

    #[inline]
    pub fn push(&mut self, _interval: Interval) {
        #[cfg(test)]
        {
            // This block makes sense only in testing mode, when we actually collect the intervals
            self.intervals.push(_interval);
        }
        self.n_matches += 1;
    }

    pub fn finalize(self) -> QueryAnswer {
        assert!(
            self.examined >= self.n_matches,
            "examined {}, matches {}",
            self.examined,
            self.n_matches
        );

        QueryAnswer {
            #[cfg(test)]
            intervals: self.intervals,
            examined: self.examined,
            n_matches: self.n_matches,
        }
    }
}

pub struct FocusResult {
    n_matches: u32,
    n_examined: u32,
    query_time: std::time::Duration,
}

pub trait Algorithm: std::fmt::Debug + DeepSizeOf {
    fn name(&self) -> String;
    fn parameters(&self) -> String;
    fn version(&self) -> u8;
    fn index(&mut self, dataset: &[Interval]);
    fn query(&self, query: &Query, answer: &mut QueryAnswerBuilder);
    /// Clears the index, freeing up space
    fn clear(&mut self);

    fn descr(&self) -> String {
        format!(
            "{} ({}) [v{}]",
            self.name(),
            self.parameters(),
            self.version()
        )
    }

    fn run_batch(&self, queries: &[Query]) -> u32 {
        // Set a timeout given by 10 queries per second
        let mut cnt = 0;
        for query in queries.iter() {
            let mut query_result = QueryAnswer::builder();
            self.query(query, &mut query_result);
            cnt += query_result.n_matches;
        }
        cnt
    }

    fn run_focus(&self, queries: &[Query], n_samples: u32) -> Vec<FocusResult> {
        let mut results = Vec::with_capacity(queries.len());
        let mut pl = ProgressLogger::builder()
            .with_expected_updates(queries.len() as u64)
            .with_items_name("queries")
            .start();
        for query in queries {
            // Get the results in terms of number of matches and number of intervals examined
            let mut query_result = QueryAnswer::builder();
            self.query(query, &mut query_result);
            let n_matches = query_result.n_matches;
            let n_examined = query_result.examined;

            // Warm up the cache etc.. in the hope of having more stable measurements
            for _ in 0..5 {
                let mut query_result = QueryAnswer::builder();
                self.query(query, &mut query_result);
            }

            // Now run the estimate
            let mut query_result = QueryAnswer::builder();
            let start = std::time::Instant::now();
            for _ in 0..n_samples {
                self.query(query, &mut query_result);
            }
            let query_time = start.elapsed() / n_samples;

            results.push(FocusResult {
                n_matches,
                n_examined,
                query_time,
            });
            pl.update(1u64);
        }
        pl.stop();

        results
    }

    /// Returns either the query results or, if running too slow,
    /// the predicted duration
    fn run(
        &self,
        queries: &[Query],
        min_qps: f64,
    ) -> std::result::Result<Vec<QueryAnswer>, std::time::Duration> {
        // Set a timeout given by 10 queries per second
        let mut result = Vec::with_capacity(queries.len());
        let mut pl = ProgressLogger::builder()
            .with_expected_updates(queries.len() as u64)
            .with_items_name("queries")
            .start();
        let mut cnt = 0;
        for query in queries.iter() {
            if let Some(throughput) = pl.throughput() {
                // info!("time to completion {:?}", time_to_completion);
                if throughput < min_qps && cnt > 100 {
                    warn!("aborting execution, throughput too small");
                    return Err(pl.time_to_completion().unwrap());
                }
            }
            let mut query_result = QueryAnswer::builder();
            self.query(query, &mut query_result);
            result.push(query_result.finalize());
            pl.update(1u64);
            cnt += 1;
        }
        pl.stop();
        Ok(result)
    }

    /// Run all the queries, and actually record the answered items
    #[cfg(test)]
    fn run_recording(&self, queries: &[Query]) -> Vec<QueryAnswer> {
        let mut result = Vec::with_capacity(queries.len());
        let mut pl = ProgressLogger::builder()
            .with_expected_updates(queries.len() as u64)
            .with_items_name("queries")
            .start();
        for query in queries.iter() {
            let mut query_result = QueryAnswer::builder().record_intervals();
            self.query(query, &mut query_result);
            result.push(query_result.finalize());
            pl.update(1u64);
        }
        pl.stop();
        result
    }

    fn index_size(&self) -> usize {
        self.deep_size_of()
    }

    fn reporter_hook(&self, _reporter: &crate::reporter::Reporter) -> anyhow::Result<()> {
        // do nothing by default
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_overlap() {
        assert!(Interval::new(0, 3).overlaps(&Interval::new(2, 3)));
        assert!(!Interval::new(0, 3).overlaps(&Interval::new(4, 3)));
        assert!(Interval::new(0, 6).overlaps(&Interval::new(4, 3)));
        assert!(Interval::new(0, 6).overlaps(&Interval::new(2, 3)));
        assert!(!Interval::new(17, 1).overlaps(&Interval::new(18, 22)))
    }
}
