use crate::triplestore::sparql::lazy_expressions::lazy_expression;
use crate::triplestore::sparql::query_context::{Context, PathEntry};
use polars::prelude::{LazyFrame};
use spargebra::algebra::OrderExpression;
use std::collections::HashSet;

pub fn lazy_order_expression(
    oexpr: &OrderExpression,
    lazy_frame: LazyFrame,
    columns: &HashSet<String>,
    context: &Context,
) -> (LazyFrame, bool, Context) {
    match oexpr {
        OrderExpression::Asc(expr) => {
            let inner_context = context.extension_with(PathEntry::OrderingOperation);
            (
                lazy_expression(expr, lazy_frame, columns,  &inner_context),
                true,
                inner_context,
            )
        }
        OrderExpression::Desc(expr) => {
            let inner_context = context.extension_with(PathEntry::OrderingOperation);
            (
                lazy_expression(expr, lazy_frame, columns,  &inner_context),
                false,
                inner_context,
            )
        }
    }
}
