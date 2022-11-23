mod constant_terms;
pub mod default;
pub mod errors;
mod validation_inference;
mod literals;

use crate::ast::{Instance, ListExpanderType, Signature, StottrTerm, Template};
use crate::constants::OTTR_TRIPLE;
use crate::document::document_from_str;
use crate::mapping::constant_terms::constant_to_expr;
use crate::mapping::errors::MappingError;
use crate::templates::TemplateDataset;
use polars::lazy::prelude::{col, Expr};
use polars::prelude::{DataFrame, IntoLazy, LazyFrame, PolarsError};
use std::collections::HashMap;
use std::error::Error;
use std::io::Write;
use std::path::Path;
use oxrdf::{NamedNode, Triple};
use crate::triplestore::Triplestore;

pub struct Mapping {
    template_dataset: TemplateDataset,
    triplestore: Triplestore
}

pub struct ExpandOptions {
    pub language_tags: Option<HashMap<String, String>>,
}

impl Default for ExpandOptions {
    fn default() -> Self {
        ExpandOptions {
            language_tags: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PrimitiveColumn {
    pub rdf_node_type: RDFNodeType,
    pub language_tag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RDFNodeType {
    IRI,
    BlankNode,
    Literal(NamedNode),
    None,
}

#[derive(Debug, PartialEq)]
pub struct MappingReport {}

impl Mapping {
    pub fn new(template_dataset: &TemplateDataset) -> Mapping {
        Mapping {
            template_dataset: template_dataset.clone(),
            triplestore: Triplestore::new()
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
        self.triplestore.write_n_triples_all_dfs(buffer, 1024).unwrap();
        Ok(())
    }

    pub fn export_oxrdf_triples(&self) -> Vec<Triple> {
        self.triplestore.export_oxrdf_triples()
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
        let columns = self.validate_infer_dataframe_columns(
            &target_template.signature,
            &df,
            &options
        )?;
        let mut result_vec = vec![];
        self._expand(&target_template_name, df.lazy(), columns, &mut result_vec)?;
        self.process_results(result_vec);

        Ok(MappingReport {})
    }

    fn _expand(
        &self,
        name: &str,
        mut lf: LazyFrame,
        columns: HashMap<String, PrimitiveColumn>,
        new_lfs_columns: &mut Vec<(LazyFrame, HashMap<String, PrimitiveColumn>)>,
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

    fn process_results(&mut self, result_vec: Vec<(LazyFrame, HashMap<String, PrimitiveColumn>)>) {
        for (lf, mut columns) in result_vec {
            let df = lf.collect().expect("Collect problem");
            let PrimitiveColumn{rdf_node_type, language_tag} = columns.remove("object").unwrap();
            self.triplestore.add_triples(df, rdf_node_type, language_tag);
        }
    }
}

fn create_remapped_lazy_frame(
    instance: &Instance,
    signature: &Signature,
    mut lf: LazyFrame,
    columns: &HashMap<String, PrimitiveColumn>,
) -> Result<(LazyFrame, HashMap<String, PrimitiveColumn>), MappingError> {
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
                let (expr, _, rdf_node_type, language_tag) = constant_to_expr(ct, &target.ptype)?;
                let mapped_column =
                    PrimitiveColumn { rdf_node_type, language_tag };
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

