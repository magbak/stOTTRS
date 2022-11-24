mod distinct;
mod extend;
mod filter;
mod group;
mod join;
mod left_join;
mod minus;
mod order_by;
mod project;
mod union;
mod lazy_triple;

use super::Triplestore;
use log::debug;
use spargebra::algebra::GraphPattern;
use crate::triplestore::sparql::errors::SparqlError;
use crate::triplestore::sparql::query_context::Context;
use crate::triplestore::sparql::solution_mapping::SolutionMappings;

impl Triplestore {
    pub(crate) fn lazy_graph_pattern(
        &mut self,
        graph_pattern: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing graph pattern at context: {}", context.as_str());

        match graph_pattern {
            GraphPattern::Bgp { .. } => Ok(solution_mappings.unwrap()),
            GraphPattern::Path { .. } => Ok(solution_mappings.unwrap()),
            GraphPattern::Join { left, right } => {
                self.lazy_join(
                    left,
                    right,
                    solution_mappings,
                    context,
                )

            }
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => {
                self.lazy_left_join(
                    left,
                    right,
                    expression,
                    solution_mappings,
                    context,
                )

            }
            GraphPattern::Filter { expr, inner } => {
                self.lazy_filter(
                    inner,
                    expr,
                    solution_mappings,
                    &context,
                )

            }
            GraphPattern::Union { left, right } => {
                self.lazy_union(
                    left,
                    right,
                    solution_mappings,
                    context,
                )

            }
            GraphPattern::Graph { name: _, inner: _ } => Ok(solution_mappings.unwrap()),
            GraphPattern::Extend {
                inner,
                variable,
                expression,
            } => {
                self.lazy_extend(
                    inner,
                    variable,
                    expression,
                    solution_mappings,
                    context,
                )

            }
            GraphPattern::Minus { left, right } => {
                self.lazy_minus(
                    left,
                    right,
                    solution_mappings,
                    context,
                )

            }
            GraphPattern::Values {
                variables: _,
                bindings: _,
            } => Ok(solution_mappings.unwrap()),
            GraphPattern::OrderBy { inner, expression } => {
                self.lazy_order_by(
                    inner,
                    expression,
                    solution_mappings,
                    context,
                )

            }
            GraphPattern::Project { inner, variables } => {
                self.lazy_project(
                    inner,
                    variables,
                    solution_mappings,
                    context,
                )

            }
            GraphPattern::Distinct { inner } => {
                self.lazy_distinct(
                    inner,
                    solution_mappings,
                    context,
                )

            }
            GraphPattern::Reduced { .. } => {
                todo!()
            }
            GraphPattern::Slice { .. } => {
                todo!()
            }
            GraphPattern::Group {
                inner,
                variables,
                aggregates,
            } => {
                self.lazy_group(
                    inner,
                    variables,
                    aggregates,
                    solution_mappings,
                    context,
                )

            }
            GraphPattern::Service { .. } => Ok(solution_mappings.unwrap()),
        }
    }
}
