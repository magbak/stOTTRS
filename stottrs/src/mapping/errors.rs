use crate::ast::{ConstantTerm, PType};
use oxrdf::IriParseError;
use polars_core::frame::DataFrame;
use polars_core::prelude::{DataType, Series};
use std::fmt::{Display, Formatter};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MappingError {
    InvalidTemplateNameError(#[from] IriParseError),
    TemplateNotFound(String),
    MissingKeyColumn,
    MissingForeignKeyColumn(String, String),
    KeyColumnContainsDuplicates(Series),
    KeyAndPathColumnOverlapsExisting(DataFrame),
    NonOptionalColumnHasNull(String, DataFrame),
    NonBlankColumnHasBlankNode(String, Series),
    MissingParameterColumn(String),
    ContainsIrrelevantColumns(Vec<String>),
    CouldNotInferStottrDatatypeForColumn(String, DataType),
    ColumnDataTypeMismatch(String, DataType, PType),
    InvalidPredicateConstant(ConstantTerm),
    PTypeNotSupported(String, PType),
    UnknownTimeZoneError(String),
    UnknownVariableError(String),
    ConstantDoesNotMatchDataType(ConstantTerm, PType, PType),
    ConstantListHasInconsistentPType(ConstantTerm, PType, PType),
    NoTemplateForTemplateNameFromPrefix(String),
}

impl Display for MappingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MappingError::TemplateNotFound(t) => {
                write!(f, "Could not find template: {}", t)
            }
            MappingError::MissingKeyColumn => {
                write!(f, "Could not find Key column")
            }
            MappingError::KeyColumnContainsDuplicates(dupes) => {
                write!(f, "Key column has duplicate entries: {}", dupes)
            }
            MappingError::KeyAndPathColumnOverlapsExisting(overlapping) => {
                write!(f, "Key and path columns overlaps existing: {}", overlapping)
            }
            MappingError::NonOptionalColumnHasNull(col, nullkey) => {
                write!(
                    f,
                    "Column {} which is non-optional has null values for keys: {}",
                    col, nullkey
                )
            }
            MappingError::NonBlankColumnHasBlankNode(col, blanks) => {
                write!(f, "Non-blank column {} has blanks {}", col, blanks)
            }
            MappingError::MissingParameterColumn(c) => {
                write!(f, "Expected column {} is missing", c)
            }
            MappingError::ContainsIrrelevantColumns(irr) => {
                write!(f, "Unexpected columns: {}", irr.join(","))
            }
            MappingError::CouldNotInferStottrDatatypeForColumn(col, dt) => {
                write!(
                    f,
                    "Could not infer stottr type for column {} with polars datatype {}",
                    col, dt
                )
            }
            MappingError::ColumnDataTypeMismatch(col, dt, ptype) => {
                write!(
                    f,
                    "Column {} had datatype {} which was incompatible with the stottr datatype {}",
                    col, dt, ptype
                )
            }
            MappingError::PTypeNotSupported(name, ptype) => {
                write!(
                    f,
                    "Found value {} with unsupported stottr datatype {}",
                    name, ptype
                )
            }
            MappingError::UnknownTimeZoneError(tz) => {
                write!(f, "Unknown time zone {}", tz)
            }
            MappingError::UnknownVariableError(v) => {
                write!(
                    f,
                    "Could not find variable {}, is the stottr template invalid?",
                    v
                )
            }
            MappingError::ConstantDoesNotMatchDataType(constant_term, expected, actual) => {
                write!(
                    f,
                    "Expected constant term {:?} to have data type {} but was {}",
                    constant_term, expected, actual
                )
            }
            MappingError::ConstantListHasInconsistentPType(constant_term, prev, next) => {
                write!(
                    f,
                    "Constant term {:?} has inconsistent data types {} and {}",
                    constant_term, prev, next
                )
            }
            MappingError::InvalidTemplateNameError(t) => {
                write!(f, "Invalid template name {}", t)
            }
            MappingError::MissingForeignKeyColumn(colname, expected_colname) => {
                write!(
                    f,
                    "Expected that the column {} occurs in place of {}",
                    expected_colname, colname
                )
            }
            MappingError::NoTemplateForTemplateNameFromPrefix(prefix) => {
                write!(
                    f,
                    "Template name {} inferred from prefix could not be found",
                    prefix
                )
            }
            MappingError::InvalidPredicateConstant(constant_term) => {
                write!(
                    f,
                    "Predicate constant {} is not valid, must be an IRI, e.g. prefix:predicate",
                    constant_term,
                )
            }
        }
    }
}
