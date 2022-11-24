use super::Triplestore;
use log::debug;
use polars::prelude::{col, Expr, IntoLazy, LiteralValue};
use spargebra::algebra::GraphPattern;
use std::ops::Not;
use crate::triplestore::sparql::errors::SparqlError;
use crate::triplestore::sparql::query_context::{Context, PathEntry};
use crate::triplestore::sparql::solution_mapping::SolutionMappings;

impl Triplestore {
    pub(crate) fn lazy_minus(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        debug!("Processing minus graph pattern");
        let left_context = context.extension_with(PathEntry::MinusLeftSide);
        let right_context = context.extension_with(PathEntry::MinusRightSide);
        let minus_column = left_context.as_str();
        let mut left_solution_mappings = self
            .lazy_graph_pattern(
                left,
                solution_mappings,
                &left_context,
            )
            ?;

        let mut left_df = left_solution_mappings.mappings
            .with_column(Expr::Literal(LiteralValue::Int64(1)).alias(&minus_column))
            .with_column(col(&minus_column).cumsum(false).keep_name())
            .collect()
            .expect("Minus collect left problem");
        left_solution_mappings.mappings = left_df.clone().lazy();
        let left_columns = left_solution_mappings.columns.clone();
        let left_datatypes = left_solution_mappings.datatypes.clone();

        //TODO: determine only variables actually used before copy
        let right_solution_mappings = self
            .lazy_graph_pattern(
                right,
                Some(left_solution_mappings),
                &right_context,
            )
            ?;

        let SolutionMappings {
            mappings: right_mappings,
            ..
        } = right_solution_mappings;

        let right_df = right_mappings
            .select([col(&minus_column)])
            .collect()
            .expect("Minus right df collect problem");
        left_df = left_df
            .filter(
                &left_df
                    .column(&minus_column)
                    .unwrap()
                    .is_in(right_df.column(&minus_column).unwrap())
                    .unwrap()
                    .not(),
            )
            .expect("Filter minus left hand side problem");
        left_df = left_df.drop(&minus_column).unwrap();
        Ok(SolutionMappings::new(
            left_df.lazy(),
            left_columns,
            left_datatypes,
        ))
    }
}
