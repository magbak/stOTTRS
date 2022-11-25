use super::Triplestore;
use polars_core::frame::UniqueKeepStrategy;
use spargebra::algebra::GraphPattern;
use log::debug;
use crate::triplestore::sparql::errors::SparqlError;
use crate::triplestore::sparql::query_context::{Context, PathEntry};
use crate::triplestore::sparql::solution_mapping::SolutionMappings;

impl Triplestore {
    pub(crate) fn lazy_distinct(
        &self,
        inner: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing distinct graph pattern");
        let SolutionMappings { mappings, columns, datatypes } = self.lazy_graph_pattern(
            inner,
            solution_mappings,
            &context.extension_with(PathEntry::DistinctInner),
        )?;
        Ok( SolutionMappings::new(mappings.unique_stable(None, UniqueKeepStrategy::First), columns, datatypes))
    }
}