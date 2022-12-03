pub(crate) mod conversion;
mod export_triples;
pub mod native_parquet_write;
mod ntriples_write;
mod parquet;
pub mod sparql;

use crate::mapping::RDFNodeType;
use crate::triplestore::parquet::{property_to_filename, write_parquet};
use log::debug;
use oxrdf::vocab::xsd;
use oxrdf::NamedNode;
use polars::prelude::{concat, IntoLazy, LazyFrame};
use polars_core::datatypes::AnyValue;
use polars_core::frame::{DataFrame, UniqueKeepStrategy};
use polars_core::prelude::DataType;
use polars_core::series::Series;
use rayon::iter::ParallelDrainRange;
use rayon::iter::ParallelIterator;
use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;
use uuid::Uuid;

const LANGUAGE_TAG_COLUMN: &str = "language_tag";

pub struct Triplestore {
    deduplicated: bool,
    caching_folder: Option<String>,
    df_map: HashMap<String, HashMap<RDFNodeType, TripleTable>>,
}

pub struct TripleTable {
    dfs: Option<Vec<DataFrame>>,
    df_paths: Option<Vec<String>>,
    unique: bool,
    call_uuid: String,
}

#[derive(PartialEq)]
enum TripleType {
    ObjectProperty,
    StringProperty,
    NonStringProperty,
}

pub struct TriplesToAdd {
    pub(crate) df: DataFrame,
    pub(crate) object_type: RDFNodeType,
    pub(crate) language_tag: Option<String>,
    pub(crate) static_verb_column: Option<String>,
    pub has_unique_subset: bool,
}

pub struct TripleDF {
    df: DataFrame,
    predicate: String,
    object_type: RDFNodeType,
}

impl Triplestore {
    pub fn new(caching_folder: Option<String>) -> Triplestore {
        Triplestore {
            df_map: HashMap::new(),
            deduplicated: true,
            caching_folder,
        }
    }

    pub fn deduplicate(&mut self) {
        let now = Instant::now();
        for (_, map) in &mut self.df_map {
            for (_, v) in map {
                if !v.unique {
                    let drained: Vec<LazyFrame> = v.dfs.drain(..).map(|x| x.lazy()).collect();
                    let mut lf = concat(drained.as_slice(), true, true).unwrap();
                    lf = lf.unique(None, UniqueKeepStrategy::First);
                    v.dfs.push(lf.collect().unwrap());
                    v.unique = true;
                }
            }
        }
        self.deduplicated = true;
        debug!("Deduplication took {} seconds", now.elapsed().as_secs_f64());
    }

    pub fn add_triples_vec(&mut self, mut ts: Vec<TriplesToAdd>, call_uuid: &String) {
        let df_vecs_to_add: Vec<Vec<TripleDF>> = ts
            .par_drain(..)
            .map(|t| {
                let TriplesToAdd {
                    df,
                    object_type,
                    language_tag,
                    static_verb_column,
                    has_unique_subset,
                } = t;
                let prepared_triples = prepare_triples(
                    df,
                    &object_type,
                    &language_tag,
                    static_verb_column,
                    has_unique_subset,
                );
                prepared_triples
            })
            .collect();
        let dfs_to_add = flatten(df_vecs_to_add);
        self.add_triples_df(dfs_to_add, call_uuid);
    }

    fn add_triples_df(&mut self, triples_df: Vec<TripleDF>, call_uuid: &String) {
        if let Some(folder) = &self.caching_folder {
            self.add_triples_df_with_folder(triples_df, call_uuid);
        } else {
            self.add_triples_df_without_folder(triples_df, call_uuid);
        }
    }

    fn add_triples_df_with_folder(&mut self, triples_df: Vec<TripleDF>, call_uuid: &String) {
        let folder_path = Path::new(folder);
        let file_paths: Vec<(Path, Result<_, _>, String, RDFNodeType)> = triples_df
            .par_drain(..)
            .map(|tdf| {
                let TripleDF {
                    mut df,
                    predicate,
                    object_type,
                } = tdf;
                let file_name = format!(
                    "{}_{}.parquet",
                    property_to_filename(&predicate),
                    Uuid::new_v4()
                );
                let mut file_path_buf = folder_path.to_path_buf();
                file_path_buf.push(file_name);
                let file_path = file_path_buf.as_path();
                (
                    file_path,
                    write_parquet(&mut df, &file_path),
                    predicate,
                    object_type,
                )
            })
            .collect();
        for (file_path, res, predicate, object_type) in file_paths {
            res?;
            //Safe to assume everything is unique
            let file_path_string = file_path.to_str().unwrap().to_string();
            if let Some(m) = self.df_map.get_mut(&predicate) {
                if let Some(v) = m.get_mut(&object_type) {
                    v.df_paths.as_mut().unwrap().push(file_path_string);
                    v.unique = v.unique && (call_uuid == &v.call_uuid);
                    if !v.unique {
                        self.deduplicated = false;
                    }
                } else {
                    m.insert(
                        object_type,
                        TripleTable {
                            dfs: None,
                            df_paths: Some(vec![file_path_string]),
                            unique: true,
                            call_uuid: call_uuid.clone(),
                        },
                    );
                }
            } else {
                self.df_map.insert(
                    predicate,
                    HashMap::from([(
                        object_type,
                        TripleTable {
                            dfs: None,
                            df_paths: Some(vec![file_path_string]),
                            unique: true,
                            call_uuid: call_uuid.clone(),
                        },
                    )]),
                );
            }
        }
    }

    fn add_triples_df_without_folder(&mut self, triples_df: Vec<TripleDF>, call_uuid: &String) {
        for TripleDF {
            df,
            predicate,
            object_type,
        } in triples_df
        {
            //Safe to assume everything is unique
            if let Some(m) = self.df_map.get_mut(&predicate) {
                if let Some(v) = m.get_mut(&object_type) {
                    v.dfs.as_mut().unwrap().push(df);
                    v.unique = v.unique && (call_uuid == &v.call_uuid);
                    if !v.unique {
                        self.deduplicated = false;
                    }
                } else {
                    m.insert(
                        object_type,
                        TripleTable {
                            dfs: Some(vec![df]),
                            df_paths: None,
                            unique: true,
                            call_uuid: call_uuid.clone(),
                        },
                    );
                }
            } else {
                self.df_map.insert(
                    predicate,
                    HashMap::from([(
                        object_type,
                        TripleTable {
                            dfs: Some(vec![df]),
                            df_paths: None,
                            unique: true,
                            call_uuid: call_uuid.clone(),
                        },
                    )]),
                );
            }
        }
    }

    fn get_object_property_triples(&self) -> Vec<(&String, &DataFrame)> {
        let mut df_vec = vec![];
        for (verb, map) in &self.df_map {
            for (k, tt) in map {
                if let &RDFNodeType::IRI = k {
                    for df in &tt.dfs {
                        df_vec.push((verb, df))
                    }
                }
            }
        }
        df_vec
    }

    fn get_mut_object_property_triples(&mut self) -> Vec<(&String, &mut DataFrame)> {
        let mut df_vec = vec![];
        for (verb, map) in &mut self.df_map {
            for (k, tt) in map {
                if let &RDFNodeType::IRI = k {
                    for df in &mut tt.dfs {
                        df_vec.push((verb, df))
                    }
                }
            }
        }
        df_vec
    }

    fn get_string_property_triples(&self) -> Vec<(&String, &DataFrame)> {
        let mut df_vec = vec![];
        for (verb, map) in &self.df_map {
            for (k, tt) in map {
                if let RDFNodeType::Literal(lit) = k {
                    if lit.as_ref() == xsd::STRING {
                        for df in &tt.dfs {
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
        for (verb, map) in &mut self.df_map {
            for (k, tt) in map {
                if let RDFNodeType::Literal(lit) = k {
                    if lit.as_ref() == xsd::STRING {
                        for df in &mut tt.dfs {
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
        for (verb, map) in &self.df_map {
            for (k, tt) in map {
                if let RDFNodeType::Literal(lit) = k {
                    if lit.as_ref() != xsd::STRING {
                        for df in &tt.dfs {
                            df_dts_vec.push((verb, df, lit))
                        }
                    }
                }
            }
        }
        df_dts_vec
    }

    fn get_mut_non_string_property_triples(
        &mut self,
    ) -> Vec<(&String, &mut DataFrame, &NamedNode)> {
        let mut df_dts_vec = vec![];
        for (verb, map) in &mut self.df_map {
            for (k, tt) in map {
                if let RDFNodeType::Literal(lit) = k {
                    if lit.as_ref() != xsd::STRING {
                        for df in &mut tt.dfs {
                            df_dts_vec.push((verb, df, lit))
                        }
                    }
                }
            }
        }
        df_dts_vec
    }
}

pub fn prepare_triples(
    mut df: DataFrame,
    object_type: &RDFNodeType,
    language_tag: &Option<String>,
    static_verb_column: Option<String>,
    has_unique_subset: bool,
) -> Vec<TripleDF> {
    let now = Instant::now();
    let mut out_df_vec = vec![];
    if df.height() == 0 {
        return vec![];
    }
    if let Some(static_verb_column) = static_verb_column {
        df = df.select(["subject", "object"]).unwrap();
        if let Some(tdf) = prepare_triples_df(
            df,
            static_verb_column,
            object_type,
            language_tag,
            has_unique_subset,
        ) {
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
            part = part.select(["subject", "object"]).unwrap();
            if let Some(tdf) = prepare_triples_df(
                part,
                predicate,
                object_type,
                language_tag,
                has_unique_subset,
            ) {
                out_df_vec.push(tdf);
            }
        }
    }
    debug!(
        "Adding triples took {} seconds",
        now.elapsed().as_secs_f32()
    );
    out_df_vec
}

fn prepare_triples_df(
    mut df: DataFrame,
    predicate: String,
    object_type: &RDFNodeType,
    language_tag: &Option<String>,
    has_unique_subset: bool,
) -> Option<TripleDF> {
    let now = Instant::now();
    df = df.drop_nulls(None).unwrap();
    if df.height() == 0 {
        return None;
    }
    debug!(
        "Prepare single triple df after drop null before it is added took {} seconds",
        now.elapsed().as_secs_f32()
    );
    if !has_unique_subset {
        df = df.unique(None, UniqueKeepStrategy::First).unwrap();
    }
    debug!(
        "Prepare single triple df unique before it is added took {} seconds",
        now.elapsed().as_secs_f32()
    );

    if let RDFNodeType::Literal(lit) = object_type {
        if lit.as_ref() == xsd::STRING {
            if let Some(tag) = language_tag {
                let lt_ser = Series::new_empty(LANGUAGE_TAG_COLUMN, &DataType::Utf8)
                    .extend_constant(AnyValue::Utf8(tag), df.height())
                    .unwrap();
                df.with_column(lt_ser).unwrap();
            } else {
                let lt_ser = Series::full_null(LANGUAGE_TAG_COLUMN, df.height(), &DataType::Utf8);
                df.with_column(lt_ser).unwrap();
            }
        }
    }
    //TODO: add polars datatype harmonization here.
    debug!(
        "Prepare single triple df before it is added took {} seconds",
        now.elapsed().as_secs_f32()
    );
    Some(TripleDF {
        df,
        predicate,
        object_type: object_type.clone(),
    })
}

//From: https://users.rust-lang.org/t/flatten-a-vec-vec-t-to-a-vec-t/24526/3
fn flatten<T>(nested: Vec<Vec<T>>) -> Vec<T> {
    nested.into_iter().flatten().collect()
}
