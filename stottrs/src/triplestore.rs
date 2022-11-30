mod ntriples_write;
mod export_triples;
pub(crate) mod conversion;
pub mod sparql;
mod formatted_parquet_write;

use std::collections::HashMap;
use std::time::Instant;
use log::debug;
use oxrdf::NamedNode;
use oxrdf::vocab::xsd;
use polars::prelude::{concat, IntoLazy, LazyFrame};
use polars_core::datatypes::AnyValue;
use polars_core::frame::{DataFrame, UniqueKeepStrategy};
use polars_core::prelude::{DataType};
use polars_core::series::Series;
use rayon::iter::ParallelDrainRange;
use rayon::iter::ParallelIterator;
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

pub struct TriplesToAdd {
    pub(crate) df:DataFrame,
    pub(crate) object_type: RDFNodeType,
    pub(crate) language_tag:Option<String>,
    pub(crate) static_verb_column:Option<String>,
    pub has_unique_subset: bool,
}

pub struct TripleDF {
    df:DataFrame,
    predicate:String,
    object_type:RDFNodeType
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

    pub fn add_triples_vec(&mut self, mut ts: Vec<TriplesToAdd>) {
        let df_vecs_to_add:Vec<Vec<TripleDF>> = ts.par_drain(..).map(|t| {
            let TriplesToAdd{ df, object_type, language_tag, static_verb_column, has_unique_subset } = t;
            let prepared_triples = prepare_triples(df, &object_type, &language_tag, static_verb_column, has_unique_subset);
            prepared_triples
        }).collect();
        let dfs_to_add = flatten(df_vecs_to_add);
        for tdf in dfs_to_add {
            let TripleDF{ df, predicate, object_type } = tdf;
            self.add_triples_df(df, predicate, object_type);
        }
    }


    fn add_triples_df(&mut self, df:DataFrame, predicate:String, object_type:RDFNodeType) {
        if let Some(m) = self.df_map.get_mut(&predicate) {
            if let Some(v) = m.get_mut(&object_type) {
                self.deduplicated = false;
                v.push(df);
            } else {
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


pub fn prepare_triples(mut df:DataFrame, object_type:&RDFNodeType, language_tag:&Option<String>, static_verb_column:Option<String>, has_unique_subset:bool) -> Vec<TripleDF> {
    let now = Instant::now();
    let mut out_df_vec = vec![];
    if df.height() == 0 {
        return vec![];
    }
    if let Some(static_verb_column) = static_verb_column {
        df = df.select(["subject", "object"]).unwrap();
        if let Some(tdf) = prepare_triples_df(df, static_verb_column, object_type, language_tag, has_unique_subset) {
            out_df_vec.push(tdf);
        }
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
            if let Some(tdf) = prepare_triples_df(part, predicate, object_type, language_tag, has_unique_subset) {
                out_df_vec.push(tdf);
            }
        }
    }
    debug!("Adding triples took {} seconds", now.elapsed().as_secs_f32());
    out_df_vec
}

fn prepare_triples_df(mut df:DataFrame, predicate:String, object_type:&RDFNodeType, language_tag:&Option<String>, has_unique_subset:bool) -> Option<TripleDF> {
    let now = Instant::now();
    df = df.drop_nulls(None).unwrap();
    if df.height() == 0 {
        return None;
    }
    debug!("Prepare single triple df after drop null before it is added took {} seconds", now.elapsed().as_secs_f32());
    if !has_unique_subset {
        df = df.unique(None, UniqueKeepStrategy::First).unwrap();
    }
    debug!("Prepare single triple df unique before it is added took {} seconds", now.elapsed().as_secs_f32());


    if let RDFNodeType::Literal(lit) = object_type {
        if lit.as_ref() == xsd::STRING {
            if let Some(tag) = language_tag {
                let lt_ser = Series::new_empty(LANGUAGE_TAG_COLUMN, &DataType::Utf8).extend_constant(AnyValue::Utf8(tag), df.height()).unwrap();
                df.with_column(lt_ser).unwrap();
            } else {
                let lt_ser = Series::full_null(LANGUAGE_TAG_COLUMN, df.height(), &DataType::Utf8);
                df.with_column(lt_ser).unwrap();
            }
        }
    }
    //TODO: add polars datatype harmonization here.
    debug!("Prepare single triple df before it is added took {} seconds", now.elapsed().as_secs_f32());
    Some(TripleDF{
        df,
        predicate,
        object_type: object_type.clone(),
    })
}

//From: https://users.rust-lang.org/t/flatten-a-vec-vec-t-to-a-vec-t/24526/3
fn flatten<T>(nested: Vec<Vec<T>>) -> Vec<T> {
    nested.into_iter().flatten().collect()
}
