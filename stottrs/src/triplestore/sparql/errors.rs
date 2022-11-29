use spargebra::ParseError;
use thiserror::Error;
use crate::mapping::RDFNodeType;
use crate::triplestore::sparql::query_context::Context;

#[derive(Error, Debug)]
pub enum SparqlError {
    #[error("SQL Parsersing Error: {0}")]
    ParseError(ParseError),
    #[error("Query type not supported")]
    QueryTypeNotSupported,
    #[error("Inconsistent datatypes")]
    InconsistentDatatypes(String, RDFNodeType, RDFNodeType, Context)
}
