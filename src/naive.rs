use crate::types::*;
use deepsize::DeepSizeOf;

#[derive(DeepSizeOf)]
pub struct LinearScan {
    dataset: Vec<Interval>,
}

impl std::fmt::Debug for LinearScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "linear-scan")
    }
}

impl LinearScan {
    pub fn new() -> Self {
        Self {
            dataset: Vec::new(),
        }
    }
}

impl Algorithm for LinearScan {
    fn name(&self) -> String {
        "linear-scan".to_owned()
    }
    fn parameters(&self) -> String {
        String::new()
    }

    fn version(&self) -> u8 {
        4
    }

    fn index(&mut self, dataset: &[Interval]) {
        self.dataset.clear();
        self.dataset.extend(dataset.iter().cloned());
        let size = self.deep_size_of();
        info!(
            "Space taken by intervals: {} bytes ({} Mb)",
            size,
            size / (1024 * 1024)
        );
    }

    fn query(&self, query: &Query, answer: &mut QueryAnswerBuilder) {
        for (i, interval) in self.dataset.iter().enumerate() {
            let matches_duration = query
                .duration
                .as_ref()
                .map(|d| d.contains(interval))
                .unwrap_or(true);
            let overlaps = query
                .range
                .as_ref()
                .map(|range| range.overlaps(interval))
                .unwrap_or(true);
            if matches_duration && overlaps {
                answer.push(self.dataset[i]);
            }
        }
        answer.inc_examined(self.dataset.len() as u32);
    }

    fn clear(&mut self) {
        self.dataset.clear();
    }
}
