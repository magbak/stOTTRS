use log::debug;
use super::Triplestore;
use oxrdf::{Variable};
use polars::prelude::{col, Expr};
use spargebra::algebra::{AggregateExpression, GraphPattern};
use crate::triplestore::sparql::errors::SparqlError;
use crate::triplestore::sparql::query_context::{Context, PathEntry};
use crate::triplestore::sparql::solution_mapping::SolutionMappings;

impl Triplestore {
    pub(crate) fn lazy_group(
        &self,
        inner: &GraphPattern,
        variables: &Vec<Variable>,
        aggregates: &Vec<(Variable, AggregateExpression)>,
        solution_mapping: Option<SolutionMappings>,
        context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing group graph pattern");
        let inner_context = context.extension_with(PathEntry::GroupInner);

        let mut output_solution_mappings = self.lazy_graph_pattern(inner, solution_mapping,  &inner_context)?;
        let by: Vec<Expr> = variables.iter().map(|v| col(v.as_str())).collect();

        let mut aggregate_expressions = vec![];
        let mut aggregate_inner_contexts = vec![];
        for i in 0..aggregates.len() {
            let aggregate_context = context.extension_with(PathEntry::GroupAggregation(i as u16));
            let (v, a) = aggregates.get(i).unwrap();
            let (aggregate_solution_mappings, expr, used_context) =
                self.sparql_aggregate_expression_as_lazy_column_and_expression(
                    v,
                    a,
                    output_solution_mappings,
                    &aggregate_context,
                )?;
            output_solution_mappings = aggregate_solution_mappings;
            aggregate_expressions.push(expr);
            if let Some(aggregate_inner_context) = used_context {
                aggregate_inner_contexts.push(aggregate_inner_context);
            }
        }
        let SolutionMappings { mut mappings, mut columns, datatypes } = output_solution_mappings;
        let grouped_mappings = mappings.groupby(by.as_slice());

        mappings = grouped_mappings
            .agg(aggregate_expressions.as_slice())
            .drop_columns(
                aggregate_inner_contexts
                    .iter()
                    .map(|x| x.as_str())
                    .collect::<Vec<&str>>(),
            );
        columns.clear();
        for v in variables {
            columns.insert(v.as_str().to_string());
        }
        for (v, _) in aggregates {
            columns.insert(v.as_str().to_string());
        }
        Ok(SolutionMappings::new(mappings, columns, datatypes))
    }
}