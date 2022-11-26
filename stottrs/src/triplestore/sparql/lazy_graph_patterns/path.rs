use super::Triplestore;
use crate::triplestore::sparql::errors::SparqlError;
use crate::triplestore::sparql::query_context::Context;
use crate::triplestore::sparql::solution_mapping::SolutionMappings;
use oxrdf::{NamedNode, Variable};
use polars::prelude::{col, IntoLazy};
use polars_core::datatypes::DataType;
use polars_core::frame::DataFrame;
use polars_core::prelude::{ChunkAgg, NamedFrom, UInt32Chunked};
use polars_core::utils::concat_df;
use spargebra::algebra::PropertyPathExpression;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use sprs::{CsMat, CsMatBase, CsMatI, CsVecBase};
use std::cmp::max;
use std::collections::hash_map::Values;
use std::collections::HashMap;
use std::ops::Add;
use polars_core::series::Series;
use polars_core::StrHashGlobal;

enum SubjectOrObject {
    Subject,
    Object,
}

type SparseMatrix = CsMatBase<u32, usize, Vec<usize>, Vec<usize>, Vec<u32>, usize>;

struct SparsePathReturn {
    sparmat: SparseMatrix,
    soo: SubjectOrObject,
    dt: DataType,
}

struct DFPathReturn {
    df: DataFrame,
    soo: SubjectOrObject,
    dt: DataType,
}

impl SubjectOrObject {
    fn flip(&self) -> SubjectOrObject {
        match self {
            &SubjectOrObject::Subject => SubjectOrObject::Object,
            &SubjectOrObject::Object => SubjectOrObject::Subject,
        }
    }
}

impl Triplestore {
    pub fn lazy_path(
        &self,
        subject: &TermPattern,
        ppe: &PropertyPathExpression,
        object: &TermPattern,
        context: &Context,
    ) -> Result<SolutionMappings, SparqlError> {
        let create_sparse = need_sparse_matrix(ppe);
        let out_df;
        let cat_df_map = self.create_cat_dfs(ppe);
        if create_sparse {
            if let Some(cat_df_map) = cat_df_map {
                let max_index = find_max_index(cat_df_map.values());
                let SparsePathReturn{ sparmat, soo, dt } = sparse_path(ppe, &cat_df_map, max_index as usize);
                let mut subject_vec = vec![];
                let mut object_vec = vec![];
                for (i, row) in sparmat.outer_iterator().enumerate() {
                    for (j,_) in row.iter() {
                        subject_vec.push(i as u32);
                        object_vec.push(j as u32);
                    }
                }
                let subject_series = Series::from_iter(subject_vec.into_iter()).cast(&DataType::Categorical(None)).unwrap();
                let object_series = Series::from_iter(object_vec.into_iter()).cast(&DataType::Categorical(None)).unwrap();
                out_df = DataFrame::new(vec![subject_series, object_series]).unwrap();
            } else {
                todo!()
            }
        }
        Ok(SolutionMappings::new(
            Default::default(),
            Default::default(),
            Default::default(),
        ))
    }

    fn create_cat_dfs(&self, ppe: &PropertyPathExpression) -> Option<HashMap<String, DataFrame>> {
        match ppe {
            PropertyPathExpression::NamedNode(nn) => {
                let df = self.get_single_nn_df(nn.as_str());
                if let Some(df) = df {
                    Some(HashMap::from([(nn.as_str().to_string(), df_with_cats(df))]))
                } else {
                    None
                }
            }
            PropertyPathExpression::Reverse(inner) => self.create_cat_dfs(inner),
            PropertyPathExpression::Sequence(left, right) => {
                if let (Some(mut left_df_map), Some(mut right_df_map)) =
                    (self.create_cat_dfs(left), self.create_cat_dfs(right))
                {
                    left_df_map.extend(right_df_map);
                    Some(left_df_map)
                } else {
                    None
                }
            }
            PropertyPathExpression::Alternative(left, right) => {
                if let (Some(mut left_df_map), Some(mut right_df_map)) =
                    (self.create_cat_dfs(left), self.create_cat_dfs(right))
                {
                    left_df_map.extend(right_df_map);
                    Some(left_df_map)
                } else {
                    None
                }
            }
            PropertyPathExpression::ZeroOrMore(inner) => self.create_cat_dfs(inner),
            PropertyPathExpression::OneOrMore(inner) => self.create_cat_dfs(inner),
            PropertyPathExpression::ZeroOrOne(inner) => self.create_cat_dfs(inner),
            PropertyPathExpression::NegatedPropertySet(nns) => {
                let lookup: Vec<_> = nns.iter().map(|x| x.as_str().to_string()).collect();
                let mut dfs = vec![];
                for nn in self.df_map.keys() {
                    if !lookup.contains(nn) {
                        let df = self.get_single_nn_df(nn);
                        if let Some(df) = df {
                            dfs.push(df_with_cats(df));
                        } else {
                            return None;
                        }
                    }
                }
                let df = concat_df(dfs.as_slice()).unwrap();
                Some(HashMap::from([(nns_name(nns), df)]))
            }
        }
    }

    fn get_single_nn_df(&self, nn: &str) -> Option<&DataFrame> {
        let map_opt = self.df_map.get(nn);
        if let Some(m) = map_opt {
            if m.is_empty() {
                panic!("Empty map should never happen");
            } else if m.len() > 1 {
                todo!("Multiple datatypes not supported yet")
            } else {
                let (dt, dfs) = m.iter().next().unwrap();
                assert_eq!(dfs.len(), 1, "Should be deduplicated");
                let df = dfs.get(0).unwrap();
                Some(df)
            }
        } else {
            None
        }
    }
}

fn df_with_cats(df: &DataFrame) -> DataFrame {
    let subject = df
        .column("subject")
        .unwrap()
        .cast(&DataType::Categorical(None))
        .unwrap();
    let object = df
        .column("object")
        .unwrap()
        .cast(&DataType::Categorical(None))
        .unwrap();
    DataFrame::new(vec![subject, object]).unwrap()
}

fn find_max_index(vals: Values<String, DataFrame>) -> u32 {
    let mut max_index = 0u32;
    for df in vals {
        if let Some(max_subject) = df
            .column("subject")
            .unwrap()
            .categorical()
            .unwrap()
            .logical()
            .max()
        {
            max_index = max(max_index, max_subject);
        }
        if let Some(max_object) = df
            .column("object")
            .unwrap()
            .categorical()
            .unwrap()
            .logical()
            .max()
        {
            max_index = max(max_index, max_object);
        }
    }
    max_index
}

fn to_csr(df: &DataFrame, max_index: usize) -> SparseMatrix {
    let mut subjects = vec![];
    for subj_arr in df
        .column("subject")
        .unwrap()
        .categorical()
        .unwrap()
        .logical()
        .downcast_iter()
    {
        for s in subj_arr {
            subjects.push(*s.unwrap() as usize);
        }
    }
    let mut objects = vec![];
    for obj_arr in df
        .column("object")
        .unwrap()
        .categorical()
        .unwrap()
        .logical()
        .downcast_iter()
    {
        for o in obj_arr {
            objects.push(*o.unwrap() as usize);
        }
    }
    let ones = vec![1_u32].repeat(subjects.len());

    let csr = CsMat::new((max_index, max_index), subjects, objects, ones);
    csr
}

fn zero_or_more(mut rel_mat: SparseMatrix) -> SparseMatrix {
    let rows = rel_mat.rows();
    let eye = SparseMatrix::eye(rows);
    rel_mat = (&rel_mat + &eye).to_csr();
    rel_mat = rel_mat.map(|x| (x > &0) as u32);
    let mut last_sum = sum_mat(&rel_mat);
    let mut fixed_point = false;
    while !fixed_point {
        rel_mat = (&rel_mat * &rel_mat).to_csr();
        rel_mat = rel_mat.map(|x| (x > &0) as u32);
        let new_sum = sum_mat(&rel_mat);
        if last_sum == new_sum {
            fixed_point = true;
        } else {
            last_sum = new_sum;
        }
    }
    rel_mat
}

fn one_or_more(mut rel_mat: SparseMatrix) -> SparseMatrix {
    let mut last_sum = sum_mat(&rel_mat);
    let mut fixed_point = false;
    while !fixed_point {
        let new_rels = (&rel_mat * &rel_mat).to_csr();
        rel_mat = (&new_rels + &rel_mat).to_csr();
        rel_mat = rel_mat.map(|x| (x > &0) as u32);
        let new_sum = sum_mat(&rel_mat);
        if last_sum == new_sum {
            fixed_point = true;
        } else {
            last_sum = new_sum;
        }
    }
    rel_mat
}

fn sum_mat(mat: &SparseMatrix) -> u32 {
    let mut s = 0;
    //Todo parallel
    for out in mat.outer_iterator() {
        s += out.data().iter().sum::<u32>()
    }
    s
}

fn need_sparse_matrix(ppe: &PropertyPathExpression) -> bool {
    match ppe {
        PropertyPathExpression::NamedNode(_) => false,
        PropertyPathExpression::Reverse(inner) => need_sparse_matrix(inner),
        PropertyPathExpression::Sequence(left, right) => {
            need_sparse_matrix(left) || need_sparse_matrix(right)
        }
        PropertyPathExpression::Alternative(a, b) => need_sparse_matrix(a) || need_sparse_matrix(b),
        PropertyPathExpression::ZeroOrMore(_) => true,
        PropertyPathExpression::OneOrMore(_) => true,
        PropertyPathExpression::ZeroOrOne(_) => false,
        PropertyPathExpression::NegatedPropertySet(_) => false,
    }
}

fn sparse_path(
    ppe: &PropertyPathExpression,
    cat_df_map: &HashMap<String, DataFrame>,
    max_index: usize,
) -> SparsePathReturn {
    match ppe {
        PropertyPathExpression::NamedNode(nn) => {
            let sparmat = to_csr(cat_df_map.get(nn.as_str()).unwrap(), max_index);
            SparsePathReturn {
                sparmat,
                soo: SubjectOrObject::Subject,
                dt: Default::default(),
            }
        }
        PropertyPathExpression::Reverse(inner) => {
            let SparsePathReturn { sparmat, soo, dt } = sparse_path(inner, cat_df_map, max_index);
            SparsePathReturn {
                sparmat: sparmat.transpose_into(),
                soo: soo.flip(),
                dt,
            }
        }
        PropertyPathExpression::Sequence(left, right) => {
            let SparsePathReturn {
                sparmat: sparmat_left,
                soo: _,
                dt: _,
            } = sparse_path(left, cat_df_map, max_index);
            let SparsePathReturn {
                sparmat: sparmat_right,
                soo: soo_right,
                dt: dt_right,
            } = sparse_path(right, cat_df_map, max_index);
            let sparmat = (&sparmat_left * &sparmat_right).to_csr();
            SparsePathReturn {
                sparmat,
                soo: soo_right,
                dt: dt_right,
            }
        }
        PropertyPathExpression::Alternative(a, b) => {
            let SparsePathReturn {
                sparmat: sparmat_a,
                soo: soo_a,
                dt: dt_a,
            } = sparse_path(a, cat_df_map, max_index);
            let SparsePathReturn {
                sparmat: sparmat_b,
                soo: soo_b,
                dt: dt_b,
            } = sparse_path(b, cat_df_map, max_index);
            let sparmat = (&sparmat_a + &sparmat_b).to_csr().map(|x| (x > &0) as u32);
            SparsePathReturn {
                sparmat,
                soo: soo_a,
                dt: dt_a,
            }
        }
        PropertyPathExpression::ZeroOrMore(inner) => {
            let SparsePathReturn {
                sparmat: sparmat_inner,
                soo: soo,
                dt: dt,
            } = sparse_path(inner, cat_df_map, max_index);
            let sparmat = zero_or_more(sparmat_inner);
            SparsePathReturn { sparmat, soo, dt }
        }
        PropertyPathExpression::OneOrMore(inner) => {
            let SparsePathReturn {
                sparmat: sparmat_inner,
                soo: soo,
                dt: dt,
            } = sparse_path(inner, cat_df_map, max_index);
            let sparmat = one_or_more(sparmat_inner);
            SparsePathReturn { sparmat, soo, dt }
        }
        PropertyPathExpression::ZeroOrOne(inner) => {
            let SparsePathReturn {
                sparmat: sparmat_inner,
                soo: soo,
                dt: dt,
            } = sparse_path(inner, cat_df_map, max_index);
            let sparmat = zero_or_more(sparmat_inner);
            SparsePathReturn { sparmat, soo, dt }
        }
        PropertyPathExpression::NegatedPropertySet(nns) => {
            let cat_df = cat_df_map.get(&nns_name(nns)).unwrap();
            let sparmat = to_csr(cat_df, max_index);
            SparsePathReturn {
                sparmat,
                soo: SubjectOrObject::Subject,
                dt: Default::default(),
            }
        }
    }
}

fn nns_name(nns: &Vec<NamedNode>) -> String {
    let mut names = vec![];
    for nn in nns {
        names.push(nn.as_str())
    }
    names.join(",")
}
