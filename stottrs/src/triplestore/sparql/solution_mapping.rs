use std::collections::{HashMap, HashSet};
use oxrdf::{Variable};
use polars::prelude::LazyFrame;
use crate::mapping::RDFNodeType;

#[derive(Clone)]
pub struct SolutionMappings {
    pub mappings: LazyFrame,
    pub columns: HashSet<String>,
    pub datatypes: HashMap<Variable, RDFNodeType>
}

impl SolutionMappings {
    pub fn new(mappings: LazyFrame, columns:HashSet<String>, datatypes: HashMap<Variable, RDFNodeType>) -> SolutionMappings {
        SolutionMappings {
            mappings,
            columns,
            datatypes
        }
    }
}