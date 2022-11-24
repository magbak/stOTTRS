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
use spargebra::Query;
use crate::triplestore::sparql::errors::SparqlError;
use crate::triplestore::sparql::solution_mapping::SolutionMappings;
use super::Triplestore;

impl Triplestore {
    pub fn query(
        &mut self,
        query: &str,
    ) -> Result<DataFrame, SparqlError> {
        let query = Query::parse(query, None).map_err(|x|SparqlError::ParseError(x))?;

        let context = Context::new();
        if let Query::Select {
            dataset: _,
            pattern,
            base_iri: _,
        } = query
        {
            let SolutionMappings{ mappings, columns:_, datatypes:_ } = self.lazy_graph_pattern(&pattern, None, &context)?;
            Ok(mappings.collect().unwrap())
        } else {
            Err(SparqlError::QueryTypeNotSupported)
        }
    }

}