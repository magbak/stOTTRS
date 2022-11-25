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
use crate::triplestore::sparql::query_context::{Context, PathEntry};
use crate::triplestore::sparql::solution_mapping::SolutionMappings;

impl Triplestore {
    pub(crate) fn lazy_graph_pattern(
        &self,
        graph_pattern: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing graph pattern at context: {}", context.as_str());

        match graph_pattern {
            GraphPattern::Bgp { patterns } => {
                let mut updated_solution_mappings = solution_mappings;
                let bgp_context = context.extension_with(PathEntry::BGP);
                for tp in patterns {
                    updated_solution_mappings = Some(self.lazy_triple_pattern(updated_solution_mappings, tp, &bgp_context)?)
                }
                Ok(updated_solution_mappings.unwrap())
            },
            GraphPattern::Path { .. } => {todo!("Not implemented yet!")},
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
            GraphPattern::Graph { name: _, inner: _ } => {todo!("Not supported")},
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
            } => {todo!("Values not supported yet")},
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
