use super::Triplestore;
use spargebra::algebra::GraphPattern;
use log::debug;
use crate::triplestore::sparql::errors::SparqlError;
use crate::triplestore::sparql::query_context::{Context, PathEntry};
use crate::triplestore::sparql::solution_mapping::SolutionMappings;

impl Triplestore {
    pub(crate) fn lazy_join(
        &self,
        left: &GraphPattern,
        right: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
    ) -> Result< SolutionMappings, SparqlError> {
        debug!("Processing join graph pattern");
        let left_context = context.extension_with(PathEntry::JoinLeftSide);
        let right_context = context.extension_with(PathEntry::JoinRightSide);

        let mut output_solution_mappings = self.lazy_graph_pattern(
            left,
            solution_mappings,
            &left_context,
        )?;
        output_solution_mappings = self.lazy_graph_pattern(
            right,
            Some(output_solution_mappings),
            &right_context,
        )?;
        Ok(output_solution_mappings)
    }
}
