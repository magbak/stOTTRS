mod ntriples_write;
mod export_triples;
pub(crate) mod conversion;

use std::collections::HashMap;
use oxrdf::NamedNode;
use oxrdf::vocab::xsd;
use polars::prelude::{concat, IntoLazy, LazyFrame};
use polars_core::datatypes::AnyValue;
use polars_core::frame::{DataFrame, UniqueKeepStrategy};
use polars_core::prelude::{DataType};
use polars_core::series::Series;
use crate::mapping::RDFNodeType;

const LANGUAGE_TAG_COLUMN:&str = "language_tag";

pub struct Triplestore {
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
        Triplestore { df_map: HashMap::new() }
    }

    fn deduplicate(&mut self) {
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
    }

    pub fn add_triples(&mut self, df:DataFrame, object_type: RDFNodeType, language_tag:Option<String>) {
        if df.height() == 0 {
            return;
        }

        let partitions = df.partition_by(["verb"]).unwrap();
            for part in partitions {
                let predicate;
                {
                    let any_predicate = part.column("verb").unwrap().get(0);
                    if let AnyValue::Utf8(p) = any_predicate {
                        predicate = p.to_string();
                    } else {
                        panic!()
                    }
                }
                self.add_triples_df(part, predicate, object_type.clone(), language_tag.clone());
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
                v.push(df);
            } else {
                m.insert(object_type, vec![df]);
            }
        } else {
            self.df_map.insert(predicate,HashMap::from([(object_type, vec![df])]));
        }
    }

    fn get_object_property_triples(&self) -> Vec<&DataFrame> {
        let mut df_vec = vec![];
        for (_,map) in &self.df_map {
            for (k,dfs) in map {
                if let &RDFNodeType::IRI = k {
                    df_vec.extend(dfs.iter())
                }
            }
        }
        df_vec
    }

    fn get_mut_object_property_triples(&mut self) -> Vec<&mut DataFrame> {
        let mut df_vec = vec![];
        for (_,map) in &mut self.df_map {
            for (k,dfs) in map {
                if let &RDFNodeType::IRI = k {
                    for df in dfs {
                        df_vec.push(df);
                    }
                }
            }
        }
        df_vec
    }

    fn get_string_property_triples(&self) -> Vec<&DataFrame> {
        let mut df_vec = vec![];
        for (_,map) in &self.df_map {
            for (k,dfs) in map {
                if let RDFNodeType::Literal(lit) = k {
                    if lit.as_ref() == xsd::STRING {
                        df_vec.extend(dfs.iter())
                    }
                }
            }
        }
        df_vec
    }

    fn get_mut_string_property_triples(&mut self) -> Vec<&mut DataFrame> {
        let mut df_vec = vec![];
        for (_,map) in &mut self.df_map {
            for (k,dfs) in map {
                if let RDFNodeType::Literal(lit) = k {
                    if lit.as_ref() == xsd::STRING {
                        for df in dfs {
                            df_vec.push(df);
                        }
                    }
                }
            }
        }
        df_vec
    }

    fn get_non_string_property_triples(&self) -> Vec<(&DataFrame, &NamedNode)> {
        let mut df_dts_vec = vec![];
        for (_,map) in &self.df_map {
            for (k,dfs) in map {
                if let RDFNodeType::Literal(lit) = k {
                    if lit.as_ref() != xsd::STRING {
                        for df in dfs {
                            df_dts_vec.push((df, lit))
                        }
                    }
                }
            }
        }
        df_dts_vec
    }

    fn get_mut_non_string_property_triples(&mut self) -> Vec<(&mut DataFrame, &NamedNode)> {
        let mut df_dts_vec = vec![];
        for (_,map) in &mut self.df_map {
            for (k,dfs) in map {
                if let RDFNodeType::Literal(lit) = k {
                    if lit.as_ref() != xsd::STRING {
                        for df in dfs {
                            df_dts_vec.push((df, lit))
                        }
                    }
                }
            }
        }
        df_dts_vec
    }
}