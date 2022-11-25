pub(crate) mod lazy_aggregate;
mod lazy_order;
mod query_context;
mod sparql_to_polars;
pub mod errors;
mod lazy_expressions;
mod lazy_graph_patterns;
pub mod solution_mapping;

use crate::triplestore::sparql::query_context::{Context};

use polars::frame::DataFrame;
use polars::prelude::{col, IntoLazy};
use polars_core::prelude::DataType;
use spargebra::Query;
use crate::triplestore::sparql::errors::SparqlError;
use crate::triplestore::sparql::solution_mapping::SolutionMappings;
use super::Triplestore;

impl Triplestore {
    pub fn query(
        &mut self,
        query: &str,
    ) -> Result<DataFrame, SparqlError> {
        if !self.deduplicated {
            self.deduplicate()
        }
        let query = Query::parse(query, None).map_err(|x|SparqlError::ParseError(x))?;

        let context = Context::new();
        if let Query::Select {
            dataset: _,
            pattern,
            base_iri: _,
        } = query
        {
            let SolutionMappings{ mappings, columns:_, datatypes:_ } = self.lazy_graph_pattern(&pattern, None, &context)?;
            let df = mappings.collect().unwrap();
            let mut cats = vec![];
            for c in df.columns(df.get_column_names()).unwrap() {
                if let DataType::Categorical(_) = c.dtype() {
                    cats.push(c.name().to_string());
                }
            }
            let mut lf = df.lazy();
            for c in cats {
                lf = lf.with_column(col(&c).cast(DataType::Utf8))
            }

            Ok(lf.collect().unwrap())
        } else {
            Err(SparqlError::QueryTypeNotSupported)
        }
    }

}