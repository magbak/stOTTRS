use crate::triplestore::sparql::query_context::Context;
use polars::prelude::{LazyFrame};
use spargebra::term::{NamedNodePattern, TriplePattern};
use std::collections::HashSet;
use super::Triplestore;

impl Triplestore {
    pub fn lazy_triple_pattern(
        &self,
        columns: &mut HashSet<String>,
        input_lf: LazyFrame,
        triple_pattern: &TriplePattern,
        context: &Context,
    ) -> LazyFrame {
        match &triple_pattern.predicate {
            NamedNodePattern::NamedNode(n) => {
                let df = self.df_map.get(k)
            }
            NamedNodePattern::Variable(v) => {}
        }
    }
}
