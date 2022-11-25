use std::collections::HashMap;
use oxrdf::{NamedNode, Variable};
use polars::prelude::IntoLazy;
use polars_core::frame::DataFrame;
use spargebra::algebra::PropertyPathExpression;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use crate::mapping::RDFNodeType;
use crate::triplestore::sparql::errors::SparqlError;
use crate::triplestore::sparql::query_context::Context;
use crate::triplestore::sparql::solution_mapping::SolutionMappings;
use super::Triplestore;

enum SubjectOrObject {
    Subject,
    Object
}

impl SubjectOrObject {
    fn flip(soo:SubjectOrObject) -> SubjectOrObject{
        match soo {
            SubjectOrObject::Subject => {SubjectOrObject::Object}
            SubjectOrObject::Object => {SubjectOrObject::Subject}
        }
    }
}


impl Triplestore {
    pub fn lazy_path(&self, subject:&TermPattern, path:&PropertyPathExpression, object:&TermPattern, context:&Context) -> Result<SolutionMappings, SparqlError> {

    }

    fn path(&self, ppe:&PropertyPathExpression, context:&Context) -> Result<Option<(DataFrame, SubjectOrObject, RDFNodeType)>, SparqlError>  {
        match ppe {
            PropertyPathExpression::NamedNode(nn) => {
                self.named_node(nn, context)
            }
            PropertyPathExpression::Reverse(inner) => {
                if let Some((mut df, s, t)) = self.path(inner, context)? {
                    df = df.lazy().rename(["subject", "object"], ["object", "subject"]).collect().unwrap();

                    Ok(Some((df, s.flip(), t.clone())))
                } else {
                    Ok(None)
                }
            }
            PropertyPathExpression::Sequence(left, right) => {}
            PropertyPathExpression::Alternative(left, right) => {}
            PropertyPathExpression::ZeroOrMore(inner) => {}
            PropertyPathExpression::OneOrMore(inner) => {}
            PropertyPathExpression::ZeroOrOne(inner) => {}
            PropertyPathExpression::NegatedPropertySet(inner) => {}
        }
    }

    fn named_node(&self, nn:&NamedNode, context:&Context) -> Result<Option<(DataFrame, SubjectOrObject, RDFNodeType)>, SparqlError> {
        let map_opt = self.df_map.get(n.as_str());
            if let Some(m) = map_opt {
                if m.is_empty() {
                    panic!("Empty map should never happen");
                } else if m.len() > 1 {
                    todo!("Multiple datatypes not supported yet")
                } else {
                    let (dt, dfs) = m.iter().next().unwrap();
                    assert_eq!(dfs.len(), 1, "Should be deduplicated");
                    let df = dfs.get(0).unwrap();
                    return Ok(Some((df.select(["subject", "object"]).unwrap(), SubjectOrObject::Object, dt.clone())))
                }
            } else {
                return Ok(None)
            }
    }
}