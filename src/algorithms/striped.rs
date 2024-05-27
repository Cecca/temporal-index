use crate::types::{Algorithm, Interval, QueryAnswer};

#[derive(Debug)]
pub struct StripedIndex {
    // a configured (but not populated) instance of the inner
    // index, to be cloned into the stripes array
    template: Box<dyn Algorithm>,
    stripes: Vec<Box<dyn Algorithm>>,
}

impl StripedIndex {
    pub fn new(template: Box<dyn Algorithm>) -> Self {
        Self {
            template,
            stripes: Vec::new(),
        }
    }
}

impl Algorithm for StripedIndex {
    fn alike(&self) -> Box<dyn Algorithm> {
        unimplemented!()
    }

    fn name(&self) -> String {
        format!("Striped({})", self.template.name())
    }

    fn parameters(&self) -> String {
        self.template.parameters()
    }

    fn version(&self) -> u8 {
        1
    }

    fn size(&self) -> usize {
        self.stripes.iter().map(|s| s.size()).sum()
    }

    fn index(&mut self, dataset: &[crate::types::Interval]) {
        use rand::prelude::*;
        let n_stripes = rayon::current_num_threads();
        let stripe_size = (dataset.len() + 1) / n_stripes;
        dbg!(stripe_size);
        let template = &self.template;
        self.stripes.clear();

        let mut dataset = dataset.to_owned();
        let mut rng = rand_xoshiro::Xoshiro256PlusPlus::seed_from_u64(123);
        dataset.shuffle(&mut rng);
        self.stripes = dataset
            .chunks(stripe_size)
            .map(|chunk| {
                let mut idx = template.alike();
                idx.index(chunk);
                idx
            })
            .collect();
    }

    fn query(&self, query: &crate::types::Query, answer: &mut crate::types::QueryAnswerBuilder) {
        self.stripes.iter().for_each(|stripe| {
            stripe.query(query, answer);
        });
    }

    fn par_query(
        &self,
        query: &crate::types::Query,
        answer: &mut crate::types::QueryAnswerBuilder,
    ) {
        use rayon::prelude::*;
        let ans = self
            .stripes
            .par_iter()
            .map(|stripe| {
                let mut partial = QueryAnswer::builder();
                stripe.query(query, &mut partial);
                partial
            })
            .reduce_with(|mut a, b| {
                a.merge(&b);
                a
            });
        answer.merge(&ans.unwrap());
    }

    fn clear(&mut self) {
        for stripe in self.stripes.iter_mut() {
            stripe.clear();
        }
        self.stripes.clear();
    }

    fn insert(&mut self, x: Interval) {
        unimplemented!()
    }

    fn remove(&mut self, x: Interval) {
        unimplemented!()
    }
}
