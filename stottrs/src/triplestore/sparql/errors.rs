use spargebra::ParseError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SparqlError {
    #[error("SQL Parsersing Error: {0}")]
    ParseError(ParseError),
    #[error("Query type not supported")]
    QueryTypeNotSupported
}
