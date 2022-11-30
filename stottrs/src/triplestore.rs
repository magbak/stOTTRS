mod ntriples_write;
mod export_triples;
pub(crate) mod conversion;
pub mod sparql;

use std::collections::HashMap;
use oxrdf::NamedNode;
use oxrdf::vocab::xsd;
use polars::prelude::{concat, IntoLazy, LazyFrame};
use polars_core::datatypes::AnyValue;
use polars_core::frame::{DataFrame, UniqueKeepStrategy};
use polars_core::prelude::{DataType};
use polars_core::series::Series;
use crate::ast::ConstantTerm;
use crate::mapping::RDFNodeType;

const LANGUAGE_TAG_COLUMN:&str = "language_tag";

pub struct Triplestore {
    deduplicated: bool,
    df_map: HashMap<String, HashMap<RDFNodeType, Vec<DataFrame>>>
}

#[derive(PartialEq)]
enum TripleType {
    ObjectProperty,
    StringProperty,
    NonStringProperty
}

impl Triplestore {
    pub fn new() -> Triplestore {
        Triplestore { df_map: HashMap::new(), deduplicated:true }
    }

    pub fn deduplicate(&mut self) {
        for (_,map) in &mut self.df_map {
            for (_,v) in map {
                if v.len() > 0 {
                    let drained: Vec<LazyFrame> = v.drain(0..v.len()).map(|x|x.lazy()).collect();
                    let mut lf = concat(drained.as_slice(), true, true).unwrap();
                    lf = lf.unique(None, UniqueKeepStrategy::First);
                    v.push(lf.collect().unwrap());
                }
            }
        }
        self.deduplicated = true;
    }

    pub fn add_triples(&mut self, mut df:DataFrame, object_type: RDFNodeType, language_tag:Option<String>, static_verb_column:Option<String>) {
        if df.height() == 0 {
            return;
        }
        if let Some(static_verb_column) = static_verb_column {
            df = df.select(["subject", "object"]).unwrap();
            self.add_triples_df(df, static_verb_column, object_type.clone(), language_tag.clone());
        } else {
            let partitions = df.partition_by(["verb"]).unwrap();
            for mut part in partitions {
                let predicate;
                {
                    let any_predicate = part.column("verb").unwrap().get(0);
                    if let AnyValue::Utf8(p) = any_predicate {
                        predicate = p.to_string();
                    } else {
                        panic!()
                    }
                }
                part  = part.select(["subject", "object"]).unwrap();
                self.add_triples_df(part, predicate, object_type.clone(), language_tag.clone());
            }
        }
    }

    fn add_triples_df(&mut self, mut df:DataFrame, predicate:String, object_type:RDFNodeType, language_tag:Option<String>) {
        df = df.drop_nulls(None).unwrap();
        if df.height() == 0 {
            return;
        }
        df = df.lazy().unique(None, UniqueKeepStrategy::First).collect().unwrap();

        if let RDFNodeType::Literal(lit) = &object_type {
            if lit.as_ref() == xsd::STRING {
                if let Some(tag) = language_tag {
                    let lt_ser = Series::new_empty(LANGUAGE_TAG_COLUMN, &DataType::Utf8).extend_constant(AnyValue::Utf8(&tag), df.height()).unwrap();
                    df.with_column(lt_ser).unwrap();
                } else {
                    let lt_ser = Series::full_null(LANGUAGE_TAG_COLUMN, df.height(), &DataType::Utf8);
                    df.with_column(lt_ser).unwrap();
                }
            }
        }
        //TODO: add polars datatype harmonization here.

        if let Some(m) = self.df_map.get_mut(&predicate) {
            if let Some(v) = m.get_mut(&object_type) {
                self.deduplicated = false;
                v.push(df);
            } else {
                //Unique here to avoid duplication
                df = df.unique(None, UniqueKeepStrategy::First).unwrap();
                m.insert(object_type, vec![df]);
            }
        } else {
            self.df_map.insert(predicate,HashMap::from([(object_type, vec![df])]));
        }
    }

    fn get_object_property_triples(&self) -> Vec<(&String, &DataFrame)> {
        let mut df_vec = vec![];
        for (verb,map) in &self.df_map {
            for (k,dfs) in map {
                if let &RDFNodeType::IRI = k {
                    for df in dfs {
                            df_vec.push((verb, df))
                        }
                }
            }
        }
        df_vec
    }

    fn get_mut_object_property_triples(&mut self) -> Vec<(&String, &mut DataFrame)> {
        let mut df_vec = vec![];
        for (verb,map) in &mut self.df_map {
            for (k,dfs) in map {
                if let &RDFNodeType::IRI = k {
                    for df in dfs {
                            df_vec.push((verb, df))
                        }
                }
            }
        }
        df_vec
    }

    fn get_string_property_triples(&self) -> Vec<(&String, &DataFrame)> {
        let mut df_vec = vec![];
        for (verb,map) in &self.df_map {
            for (k,dfs) in map {
                if let RDFNodeType::Literal(lit) = k {
                    if lit.as_ref() == xsd::STRING {
                        for df in dfs {
                            df_vec.push((verb, df))
                        }
                    }
                }
            }
        }
        df_vec
    }

    fn get_mut_string_property_triples(&mut self) -> Vec<(&String, &mut DataFrame)> {
        let mut df_vec = vec![];
        for (verb,map) in &mut self.df_map {
            for (k,dfs) in map {
                if let RDFNodeType::Literal(lit) = k {
                    if lit.as_ref() == xsd::STRING {
                        for df in dfs {
                            df_vec.push((verb, df))
                        }
                    }
                }
            }
        }
        df_vec
    }

    fn get_non_string_property_triples(&self) -> Vec<(&String, &DataFrame, &NamedNode)> {
        let mut df_dts_vec = vec![];
        for (verb,map) in &self.df_map {
            for (k,dfs) in map {
                if let RDFNodeType::Literal(lit) = k {
                    if lit.as_ref() != xsd::STRING {
                        for df in dfs {
                            df_dts_vec.push((verb, df, lit))
                        }
                    }
                }
            }
        }
        df_dts_vec
    }

    fn get_mut_non_string_property_triples(&mut self) -> Vec<(&String, &mut DataFrame, &NamedNode)> {
        let mut df_dts_vec = vec![];
        for (verb,map) in &mut self.df_map {
            for (k,dfs) in map {
                if let RDFNodeType::Literal(lit) = k {
                    if lit.as_ref() != xsd::STRING {
                        for df in dfs {
                            df_dts_vec.push((verb, df, lit))
                        }
                    }
                }
            }
        }
        df_dts_vec
    }
}