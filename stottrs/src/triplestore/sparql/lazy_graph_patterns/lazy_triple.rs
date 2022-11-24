use crate::triplestore::sparql::query_context::Context;
use polars::prelude::{col, Expr};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use std::collections::{HashMap, HashSet};
use log::warn;
use oxrdf::vocab::xsd;
use polars_core::datatypes::DataType;
use polars_core::prelude::JoinType;
use polars::prelude::IntoLazy;
use polars_core::frame::DataFrame;
use crate::mapping::RDFNodeType;
use crate::triplestore::sparql::errors::SparqlError;
use crate::triplestore::sparql::solution_mapping::SolutionMappings;
use crate::triplestore::sparql::sparql_to_polars::{sparql_literal_to_polars_literal_value, sparql_named_node_to_polars_literal_value};
use super::Triplestore;

impl Triplestore {
    pub fn lazy_triple_pattern(
        &self,
        solution_mappings: Option<SolutionMappings>,
        triple_pattern: &TriplePattern,
        _context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        match &triple_pattern.predicate {
            NamedNodePattern::NamedNode(n) => {
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
                        let mut lf = df.clone().lazy();
                        let mut var_cols = vec![];
                        let mut str_cols = vec![];
                        match &triple_pattern.subject {
                            TermPattern::NamedNode(nn) => {
                                lf = lf.filter(col("subject").eq(Expr::Literal(sparql_named_node_to_polars_literal_value(nn)))).drop_columns(["subject"])
                            }
                            TermPattern::Literal(lit) => {
                                lf = lf.filter(col("subject").eq(Expr::Literal(sparql_literal_to_polars_literal_value(lit)))).drop_columns(["subject"])
                            }
                            TermPattern::Variable(var) => {
                                lf = lf.rename(["subject"], [var.as_str()]);
                                str_cols.push(var.as_str().to_string());
                                var_cols.push(var.as_str().to_string());
                            }
                            _ => {todo!("No support for {}", &triple_pattern.object)}
                        }
                        match &triple_pattern.object {
                            TermPattern::NamedNode(nn) => {
                                lf = lf.filter(col("object").eq(Expr::Literal(sparql_named_node_to_polars_literal_value(nn)))).drop_columns(["object"])
                            }
                            TermPattern::Literal(lit) => {
                                lf = lf.filter(col("object").eq(Expr::Literal(sparql_literal_to_polars_literal_value(lit)))).drop_columns(["object"])
                            }
                            TermPattern::Variable(var) => {
                                lf = lf.rename(["subject"], [var.as_str()]);
                                var_cols.push(var.as_str().to_string());
                                match dt {
                                    RDFNodeType::IRI => {
                                        str_cols.push(var.as_str().to_string());
                                    }
                                    RDFNodeType::Literal(lit) => {
                                        if lit.as_ref() == xsd::STRING {
                                            str_cols.push(var.as_str().to_string());
                                        }

                                    }
                                    _ => {panic!("No support for this datatype: {:?}", dt)}
                                }
                            }
                            _ => {todo!("No support for {}", &triple_pattern.object)}
                        }
                        if let Some(mut mappings) = solution_mappings {
                            for c in &var_cols {
                                mappings.columns.insert(c.to_string());
                            }
                            if let TermPattern::Variable(v) = &triple_pattern.subject {
                                mappings.datatypes.insert(v.clone(), RDFNodeType::IRI);
                            }
                            if let TermPattern::Variable(v) = &triple_pattern.object {
                                mappings.datatypes.insert(v.clone(), dt.clone());
                            }

                            let mut join_cols:Vec<String> = var_cols.clone().into_iter().filter(|x| mappings.columns.contains(x)).collect();

                            for s in str_cols {
                                if join_cols.contains(&s) {
                                    lf = lf.with_column(col(&s).cast(DataType::Categorical(None)));
                                    mappings.mappings = mappings.mappings.with_column(col(&s).cast(DataType::Categorical(None)));
                                }
                            }

                            let join_on: Vec<Expr> = join_cols.iter().map(|x| col(x)).collect();

                            if join_on.is_empty() {
                                mappings.mappings = mappings.mappings.join(lf, join_on.as_slice(), join_on.as_slice(), JoinType::Cross);
                            } else {
                                mappings.mappings = mappings.mappings.join(lf, join_on.as_slice(), join_on.as_slice(), JoinType::Inner);
                            }
                            return Ok(mappings)
                        } else {
                            let mut datatypes = HashMap::new();
                            if let TermPattern::Variable(v) = &triple_pattern.subject {
                                datatypes.insert(v.clone(), RDFNodeType::IRI);
                            }
                            if let TermPattern::Variable(v) = &triple_pattern.object {
                                datatypes.insert(v.clone(), dt.clone());
                            }

                            return Ok(SolutionMappings {
                                mappings: lf,
                                columns: var_cols.into_iter().map(|x|x.to_string()).collect(),
                                datatypes
                            })
                        }
                    }
                } else {
                    warn!("Could not find triples for predicate {:?}", n);
                    return Ok(SolutionMappings::new(DataFrame::empty().lazy(), HashSet::new(), HashMap::new()))
                }
            }
            NamedNodePattern::Variable(..) => {
                todo!("Not supported yet")
            }
        }
    }
}
