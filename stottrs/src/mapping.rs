pub mod errors;
pub mod export_triples;
mod ntriples_write;
mod validation_inference;

use crate::ast::{
    ConstantLiteral, ConstantTerm, Instance, ListExpanderType, PType, Signature, StottrTerm,
    Template,
};
use crate::constants::{BLANK_NODE_IRI, NONE_IRI, OTTR_TRIPLE};
use crate::document::document_from_str;
use crate::mapping::errors::MappingError;
use crate::mapping::validation_inference::{MappedColumn, PrimitiveColumn, RDFNodeType};
use crate::templates::TemplateDataset;
use oxrdf::vocab::xsd;
use oxrdf::NamedNode;
use polars::lazy::prelude::{col, Expr, LiteralValue};
use polars::prelude::{
    concat_lst, AnyValue, DataFrame, DataType, IntoLazy, LazyFrame, PolarsError,
    Series, SpecialEq,
};
use polars::prelude::{IntoSeries, StructChunked};
use std::collections::HashMap;
use std::error::Error;
use std::io::Write;
use std::ops::{Deref, Not};
use std::path::Path;

pub struct Mapping {
    template_dataset: TemplateDataset,
    object_property_triples: Vec<DataFrame>,
    data_property_triples: Vec<DataFrame>,
}

pub struct ExpandOptions {
    pub language_tags: Option<HashMap<String, String>>
}

impl Default for ExpandOptions {
    fn default() -> Self {
        ExpandOptions {
            language_tags: None,
        }
    }
}

enum TripleType {
    ObjectProperty,
    DataProperty
}

#[derive(Debug, PartialEq)]
pub struct MappingReport {
}

impl Mapping {
    pub fn new(template_dataset: &TemplateDataset) -> Mapping {
        Mapping {
            template_dataset: template_dataset.clone(),
            object_property_triples: vec![],
            data_property_triples: vec![],
        }
    }

    pub fn from_folder<P: AsRef<Path>>(path: P) -> Result<Mapping, Box<dyn Error>> {
        let dataset = TemplateDataset::from_folder(path)?;
        Ok(Mapping::new(&dataset))
    }

    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Mapping, Box<dyn Error>> {
        let dataset = TemplateDataset::from_file(path)?;
        Ok(Mapping::new(&dataset))
    }

    pub fn from_str(s: &str) -> Result<Mapping, Box<dyn Error>> {
        let doc = document_from_str(s.into())?;
        let dataset = TemplateDataset::new(vec![doc])?;
        Ok(Mapping::new(&dataset))
    }

    pub fn from_strs(ss: Vec<&str>) -> Result<Mapping, Box<dyn Error>> {
        let mut docs = vec![];
        for s in ss {
            let doc = document_from_str(s.into())?;
            docs.push(doc);
        }
        let dataset = TemplateDataset::new(docs)?;
        Ok(Mapping::new(&dataset))
    }

    pub fn write_n_triples(&mut self, buffer: &mut dyn Write) -> Result<(), PolarsError> {
        self.write_n_triples_all_dfs(buffer, 1024).unwrap();
        Ok(())
    }

    fn resolve_template(&self, s: &str) -> Result<&Template, MappingError> {
        if let Some(t) = self.template_dataset.get(s) {
            return Ok(t);
        } else {
            let mut split_colon = s.split(":");
            let prefix_maybe = split_colon.next();
            if let Some(prefix) = prefix_maybe {
                if let Some(nn) = self.template_dataset.prefix_map.get(prefix) {
                    let possible_template_name = nn.as_str().to_string()
                        + split_colon.collect::<Vec<&str>>().join(":").as_str();
                    if let Some(t) = self.template_dataset.get(&possible_template_name) {
                        return Ok(t);
                    } else {
                        return Err(MappingError::NoTemplateForTemplateNameFromPrefix(
                            possible_template_name,
                        ));
                    }
                }
            }
        }
        Err(MappingError::TemplateNotFound(s.to_string()))
    }

    pub fn expand(
        &mut self,
        template: &str,
        df: DataFrame,
        options: ExpandOptions,
    ) -> Result<MappingReport, MappingError> {
        let target_template = self.resolve_template(template)?.clone();
        let target_template_name = target_template.signature.template_name.as_str().to_string();
        let (df, columns) = self.find_validate_and_prepare_dataframe_columns(
            &target_template.signature,
            df,
            &options,
        )?;
        let mut result_vec = vec![];
        self._expand(&target_template_name, df.lazy(), columns, &mut result_vec)?;
        self.process_results(result_vec);

        Ok(MappingReport { })
    }

    fn _expand(
        &self,
        name: &str,
        mut lf: LazyFrame,
        columns: HashMap<String, MappedColumn>,
        new_lfs_columns: &mut Vec<(LazyFrame, HashMap<String, MappedColumn>)>,
    ) -> Result<(), MappingError> {
        //At this point, the lf should have columns with names appropriate for the template to be instantiated (named_node).
        if let Some(template) = self.template_dataset.get(name) {
            if template.signature.template_name.as_str() == OTTR_TRIPLE {
                let keep_cols = vec![col("subject"), col("verb"), col("object")];
                lf = lf.select(keep_cols.as_slice());
                new_lfs_columns.push((lf, columns));
                Ok(())
            } else {
                for i in &template.pattern_list {
                    let target_template =
                        self.template_dataset.get(i.template_name.as_str()).unwrap();
                    let (instance_lf, instance_columns) = create_remapped_lazy_frame(
                        i,
                        &target_template.signature,
                        lf.clone(),
                        &columns,
                    )?;
                    self._expand(
                        i.template_name.as_str(),
                        instance_lf,
                        instance_columns,
                        new_lfs_columns,
                    )?;
                }
                Ok(())
            }
        } else {
            Err(MappingError::TemplateNotFound(name.to_string()))
        }
    }

    fn process_results(&mut self, result_vec: Vec<(LazyFrame, HashMap<String, MappedColumn>)>) {
        let mut object_properties = vec![];
        let mut data_properties = vec![];
        for (lf, columns) in result_vec {
            let mut df = lf.collect().expect("Collect problem");
            match columns.get("object").unwrap() {
                MappedColumn::PrimitiveColumn(c) => match c.rdf_node_type {
                    RDFNodeType::IRI => {
                        df = df
                            .drop_nulls(Some(&["subject".to_string(), "object".to_string()]))
                            .unwrap();
                        object_properties.push(df);
                    }
                    RDFNodeType::BlankNode => {}
                    RDFNodeType::Literal => {
                        let lexical_form_null = df
                            .column("object")
                            .unwrap()
                            .struct_()
                            .unwrap()
                            .field_by_name("lexical_form")
                            .unwrap()
                            .is_null();
                        df = df.filter(&lexical_form_null.not()).unwrap();
                        df = df
                            .drop_nulls(Some(&["subject".to_string(), "verb".to_string()]))
                            .unwrap();
                        data_properties.push(df);
                    }
                    RDFNodeType::None => {}
                },
            }
        }
        self.object_property_triples.extend(object_properties);
        self.data_property_triples.extend(data_properties);
    }
}

fn create_remapped_lazy_frame(
    instance: &Instance,
    signature: &Signature,
    mut lf: LazyFrame,
    columns: &HashMap<String, MappedColumn>,
) -> Result<(LazyFrame, HashMap<String, MappedColumn>), MappingError> {
    let mut new_map = HashMap::new();
    let mut existing = vec![];
    let mut new = vec![];
    let mut expressions = vec![];
    let mut to_expand = vec![];
    for (original, target) in instance
        .argument_list
        .iter()
        .zip(signature.parameter_list.iter())
    {
        if original.list_expand {
            to_expand.push(target.stottr_variable.name.clone());
        }
        match &original.term {
            StottrTerm::Variable(v) => {
                existing.push(v.name.clone());
                new.push(target.stottr_variable.name.clone());
                if let Some(c) = columns.get(&v.name) {
                    new_map.insert(target.stottr_variable.name.clone(), c.clone());
                } else {
                    return Err(MappingError::UnknownVariableError(v.name.clone()));
                }
            }
            StottrTerm::ConstantTerm(ct) => {
                let (expr, _, rdf_node_type) = constant_to_expr(ct, &target.ptype)?;
                let mapped_column =
                    MappedColumn::PrimitiveColumn(PrimitiveColumn { rdf_node_type });
                expressions.push(expr.alias(&target.stottr_variable.name));
                new_map.insert(target.stottr_variable.name.clone(), mapped_column);
            }
            StottrTerm::List(_) => {}
        }
    }
    let mut drop = vec![];
    for c in columns.keys() {
        if !existing.contains(c) {
            drop.push(c);
        }
    }
    if drop.len() > 0 {
        lf = lf.drop_columns(drop.as_slice());
    }

    lf = lf.rename(existing.as_slice(), new.as_slice());
    let new_column_expressions: Vec<Expr> = new.into_iter().map(|x| col(&x)).collect();
    lf = lf.select(new_column_expressions.as_slice());
    for e in expressions {
        lf = lf.with_column(e);
    }
    if let Some(le) = &instance.list_expander {
        let to_expand_cols: Vec<Expr> = to_expand.iter().map(|x| col(x)).collect();
        match le {
            ListExpanderType::Cross => {
                for c in to_expand_cols {
                    lf = lf.explode(vec![c]);
                }
            }
            ListExpanderType::ZipMin => {
                lf = lf.explode(to_expand_cols.clone());
                lf = lf.drop_nulls(Some(to_expand_cols));
            }
            ListExpanderType::ZipMax => {
                lf = lf.explode(to_expand_cols);
            }
        }
    }
    Ok((lf, new_map))
}

fn constant_to_expr(
    constant_term: &ConstantTerm,
    ptype_opt: &Option<PType>,
) -> Result<(Expr, PType, RDFNodeType), MappingError> {
    let (expr, ptype, rdf_node_type) = match constant_term {
        ConstantTerm::Constant(c) => match c {
            ConstantLiteral::IRI(iri) => (
                Expr::Literal(LiteralValue::Utf8(iri.as_str().to_string())),
                PType::BasicType(xsd::ANY_URI.into_owned()),
                RDFNodeType::IRI,
            ),
            ConstantLiteral::BlankNode(bn) => (
                Expr::Literal(LiteralValue::Utf8(bn.as_str().to_string())),
                PType::BasicType(NamedNode::new_unchecked(BLANK_NODE_IRI)),
                RDFNodeType::BlankNode,
            ),
            ConstantLiteral::Literal(lit) => {
                let value_series = Series::new_empty("lexical_form", &DataType::Utf8)
                    .extend_constant(AnyValue::Utf8(lit.value.as_str()), 1)
                    .unwrap();
                let language_tag;
                if let Some(tag) = &lit.language {
                    language_tag = tag.as_str();
                } else {
                    language_tag = "";
                }
                let language_series = Series::new_empty(&"language_tag", &DataType::Utf8)
                    .extend_constant(AnyValue::Utf8(language_tag), 1)
                    .unwrap();
                let data_type_series = Series::new_empty("datatype_iri", &DataType::Utf8)
                    .extend_constant(
                        AnyValue::Utf8(lit.data_type_iri.as_ref().unwrap().as_str()),
                        1,
                    )
                    .unwrap();
                let struct_series = StructChunked::new(
                    "stuct_chunked",
                    &[value_series, language_series, data_type_series],
                )
                .unwrap()
                .into_series();

                (
                    Expr::Literal(LiteralValue::Series(SpecialEq::new(struct_series))),
                    PType::BasicType(lit.data_type_iri.as_ref().unwrap().clone()),
                    RDFNodeType::Literal,
                )
            }
            ConstantLiteral::None => (
                Expr::Literal(LiteralValue::Null),
                PType::BasicType(NamedNode::new_unchecked(NONE_IRI)),
                RDFNodeType::None,
            ),
        },
        ConstantTerm::ConstantList(inner) => {
            let mut expressions = vec![];
            let mut last_ptype = None;
            let mut last_rdf_node_type = None;
            for ct in inner {
                let (constant_expr, actual_ptype, rdf_node_type) = constant_to_expr(ct, ptype_opt)?;
                if last_ptype.is_none() {
                    last_ptype = Some(actual_ptype);
                } else if last_ptype.as_ref().unwrap() != &actual_ptype {
                    return Err(MappingError::ConstantListHasInconsistentPType(
                        constant_term.clone(),
                        last_ptype.as_ref().unwrap().clone(),
                        actual_ptype.clone(),
                    ));
                }
                last_rdf_node_type = Some(rdf_node_type);
                expressions.push(constant_expr);
            }
            let out_ptype = PType::ListType(Box::new(last_ptype.unwrap()));
            let out_rdf_node_type = last_rdf_node_type.as_ref().unwrap().clone();

            //Workaround for ArrowError(NotYetImplemented("Cannot cast to struct from other types"))
            if last_rdf_node_type.as_ref().unwrap() == &RDFNodeType::Literal {
                let mut all_series = vec![];
                for ex in &expressions {
                    if let Expr::Literal(inner) = ex {
                        if let LiteralValue::Series(series) = inner {
                            all_series.push(series.deref().clone())
                        } else {
                            panic!("Should never happen");
                        }
                    } else {
                        panic!("Should also never happen");
                    }
                }
                let mut first = all_series.remove(0);
                for s in &all_series {
                    first.append(s).unwrap();
                }
                let out_series = first.to_list().unwrap().into_series();
                (
                    Expr::Literal(LiteralValue::Series(SpecialEq::new(out_series))),
                    out_ptype,
                    out_rdf_node_type,
                )
            } else {
                (concat_lst(expressions), out_ptype, out_rdf_node_type)
            }
        }
    };
    if let Some(ptype_in) = ptype_opt {
        if ptype_in != &ptype {
            return Err(MappingError::ConstantDoesNotMatchDataType(
                constant_term.clone(),
                ptype_in.clone(),
                ptype.clone(),
            ));
        }
    }
    Ok((expr, ptype, rdf_node_type))
}

fn is_blank_node(s: &str) -> bool {
    s.starts_with("_:")
}

