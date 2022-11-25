use oxrdf::Variable;
use polars::prelude::{col, Expr, LazyFrame};
use spargebra::algebra::{AggregateExpression, GraphPattern};
use crate::triplestore::sparql::errors::SparqlError;
use crate::triplestore::sparql::query_context::{Context, PathEntry};
use super::Triplestore;

impl Triplestore {
    fn lazy_group(
        &mut self,
        inner: &Box<GraphPattern>,
        variables: &Vec<Variable>,
        aggregates: &Vec<(Variable, AggregateExpression)>,
        context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        let mut lazy_inner = self.lazy_graph_pattern(
            inner,
            &context.extension_with(PathEntry::GroupInner),
        )?;
        let by: Vec<Expr> = variables.iter().map(|v| col(v.as_str())).collect();

        let mut column_variables = vec![];

        let mut aggregate_expressions = vec![];
        let mut aggregate_inner_contexts = vec![];
        for i in 0..aggregates.len() {
            let aggregate_context = context.extension_with(PathEntry::GroupAggregation(i as u16));
            let (v, a) = aggregates.get(i).unwrap();
            let (lf, expr, used_context) =
                sparql_aggregate_expression_as_lazy_column_and_expression(
                    v,
                    a,
                    &column_variables,
                    columns,
                    lazy_inner,
                    &aggregate_context,
                )?;
            lazy_inner = lf;
            aggregate_expressions.push(expr);
            if let Some(aggregate_inner_context) = used_context {
                aggregate_inner_contexts.push(aggregate_inner_context);
            }
        }

        let lazy_group_by = lazy_inner.groupby(by.as_slice());

        let aggregated_lf = lazy_group_by
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
        Ok(aggregated_lf)
    }
}