use oxrdf::Variable;
use super::Triplestore;
use spargebra::algebra::{Expression, GraphPattern};
use log::debug;
use crate::triplestore::sparql::errors::SparqlError;
use crate::triplestore::sparql::query_context::{Context, PathEntry};
use crate::triplestore::sparql::solution_mapping::SolutionMappings;

impl Triplestore {
    pub(crate) fn lazy_extend(
        &mut self,
        inner: &GraphPattern,
        variable: &Variable,
        expression: &Expression,
        input_solution_mappings: Option<SolutionMappings>,
        context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing extend graph pattern");
        let inner_context = context.extension_with(PathEntry::ExtendInner);
        let expression_context = context.extension_with(PathEntry::ExtendExpression);

        let mut output_solution_mappings =
            self.lazy_graph_pattern(inner, input_solution_mappings, &inner_context)?;

        if !output_solution_mappings.columns.contains(variable.as_str()) {
            output_solution_mappings = self.lazy_expression(expression, output_solution_mappings, &expression_context)?;
            output_solution_mappings.mappings = output_solution_mappings.mappings.rename([expression_context.as_str()], &[variable.as_str()]);
            output_solution_mappings.columns.insert(variable.as_str().to_string());
        }
        Ok(output_solution_mappings)
    }
}
