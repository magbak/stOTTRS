mod constant_terms;
pub mod default;
pub mod errors;
mod validation_inference;

use crate::ast::{ConstantLiteral, ConstantTerm, Instance, ListExpanderType, PType, Signature, StottrTerm, Template};
use crate::constants::OTTR_TRIPLE;
use crate::document::document_from_str;
use crate::mapping::constant_terms::constant_to_expr;
use crate::mapping::errors::MappingError;
use crate::templates::TemplateDataset;
use crate::triplestore::Triplestore;
use oxrdf::vocab::xsd;
use oxrdf::{NamedNode, NamedNodeRef, Triple};
use polars::lazy::prelude::{col, Expr};
use polars::prelude::{DataFrame, IntoLazy, LazyFrame, PolarsError};
use rayon::iter::ParallelDrainRange;
use rayon::iter::ParallelIterator;
use std::collections::HashMap;
use std::error::Error;
use std::io::Write;
use std::path::Path;

pub struct Mapping {
    template_dataset: TemplateDataset,
    pub triplestore: Triplestore,
}

pub struct ExpandOptions {
    pub language_tags: Option<HashMap<String, String>>,
}

struct OTTRTripleInstance {
    lf: LazyFrame,
    dynamic_columns: HashMap<String, PrimitiveColumn>,
    static_columns: HashMap<String, StaticColumn>,
}

#[derive(Clone)]
struct StaticColumn {
    constant_term: ConstantTerm,
    ptype: Option<PType>,
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

impl RDFNodeType {
    pub fn is_lit_type(&self, nnref: NamedNodeRef) -> bool {
        if let RDFNodeType::Literal(l) = self {
            if l.as_ref() == nnref {
                return true;
            }
        }
        false
    }

    pub fn is_bool(&self) -> bool {
        self.is_lit_type(xsd::BOOLEAN)
    }

    pub fn is_float(&self) -> bool {
        self.is_lit_type(xsd::FLOAT)
    }
}

#[derive(Debug, PartialEq)]
pub struct MappingReport {}

impl Mapping {
    pub fn new(template_dataset: &TemplateDataset) -> Mapping {
        Mapping {
            template_dataset: template_dataset.clone(),
            triplestore: Triplestore::new(),
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
        self.triplestore
            .write_n_triples_all_dfs(buffer, 1024)
            .unwrap();
        Ok(())
    }

    pub fn export_oxrdf_triples(&mut self) -> Vec<Triple> {
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
        let columns =
            self.validate_infer_dataframe_columns(&target_template.signature, &df, &options)?;
        let mut result_vec = vec![];
        self._expand(
            &target_template_name,
            df.lazy(),
            columns,
            HashMap::new(),
            &mut result_vec,
        )?;
        self.process_results(result_vec);

        Ok(MappingReport {})
    }

    fn _expand(
        &self,
        name: &str,
        mut lf: LazyFrame,
        dynamic_columns: HashMap<String, PrimitiveColumn>,
        static_columns: HashMap<String, StaticColumn>,
        new_lfs_columns: &mut Vec<OTTRTripleInstance>,
    ) -> Result<(), MappingError> {
        //At this point, the lf should have columns with names appropriate for the template to be instantiated (named_node).
        if let Some(template) = self.template_dataset.get(name) {
            if template.signature.template_name.as_str() == OTTR_TRIPLE {
                new_lfs_columns.push(OTTRTripleInstance {
                    lf,
                    dynamic_columns,
                    static_columns,
                });
                Ok(())
            } else {
                for i in &template.pattern_list {
                    let target_template =
                        self.template_dataset.get(i.template_name.as_str()).unwrap();
                    let (instance_lf, instance_dynamic_columns, instance_static_columns) =
                        create_remapped(
                            i,
                            &target_template.signature,
                            lf.clone(),
                            &dynamic_columns,
                            &static_columns,
                        )?;

                    self._expand(
                        i.template_name.as_str(),
                        instance_lf,
                        instance_dynamic_columns,
                        instance_static_columns,
                        new_lfs_columns,
                    )?;
                }
                Ok(())
            }
        } else {
            Err(MappingError::TemplateNotFound(name.to_string()))
        }
    }

    fn process_results(&mut self, mut result_vec: Vec<OTTRTripleInstance>) -> Result<(), MappingError> {
        let triples: Vec<Result<(DataFrame, RDFNodeType, Option<String>, Option<String>), MappingError>> = result_vec
            .par_drain(..)
            .map(|i| create_triples(i))
            .collect();
        let mut ok_triples = vec![];
        for t in triples {
            ok_triples.push(t?);
        }
        for (df, rdf_node_type, language_tag, verb) in ok_triples {
            self.triplestore
                .add_triples(df, rdf_node_type, language_tag, verb)
        }
        Ok(())
    }
}
fn create_triples(i: OTTRTripleInstance) -> Result<(DataFrame, RDFNodeType, Option<String>, Option<String>), MappingError>{
    let OTTRTripleInstance {
        mut lf,
        mut dynamic_columns,
        static_columns,
    } = i;

    let mut expressions = vec![];

    let mut verb = None;
    for (k, sc) in static_columns {
        if k == "verb" {
            if let ConstantTerm::Constant(c) = &sc.constant_term {
                if let ConstantLiteral::IRI(nn) = c {
                    verb = Some(nn.as_str().to_string());
                } else {
                    return Err(MappingError::InvalidPredicateConstant(sc.constant_term.clone()))
                }
            } else {
                return Err(MappingError::InvalidPredicateConstant(sc.constant_term.clone()))
            }
        } else {
            let (expr, mapped_column) = create_dynamic_expression_from_static(&k, &sc.constant_term, &sc.ptype)?;
            expressions.push(expr.alias(&k));
            dynamic_columns.insert(k, mapped_column);
        }
    }

    for e in expressions {
        lf = lf.with_column(e);
    }

    let mut keep_cols = vec![col("subject"), col("object")];
    if verb.is_none() {
        keep_cols.push(col("verb"));
    }
    lf = lf.select(keep_cols.as_slice());
    let df = lf.collect().expect("Collect problem");
    let PrimitiveColumn {
        rdf_node_type,
        language_tag,
    } = dynamic_columns.remove("object").unwrap();
    Ok((df, rdf_node_type, language_tag, verb))
}

fn create_dynamic_expression_from_static(column_name:&str, constant_term: &ConstantTerm, ptype:&Option<PType>) -> Result<(Expr, PrimitiveColumn), MappingError>{
    let (mut expr, _, rdf_node_type, language_tag) =
            constant_to_expr(constant_term, ptype)?;
    let mapped_column = PrimitiveColumn {
        rdf_node_type,
        language_tag,
    };
    expr = expr.alias(column_name);
    Ok((expr, mapped_column))
}

fn create_remapped(
    instance: &Instance,
    signature: &Signature,
    mut lf: LazyFrame,
    dynamic_columns: &HashMap<String, PrimitiveColumn>,
    constant_columns: &HashMap<String, StaticColumn>,
) -> Result<
    (
        LazyFrame,
        HashMap<String, PrimitiveColumn>,
        HashMap<String, StaticColumn>,
    ),
    MappingError,
> {
    let mut new_dynamic_columns = HashMap::new();
    let mut new_constant_columns = HashMap::new();
    let mut existing = vec![];
    let mut new = vec![];
    let mut to_expand = vec![];
    for (original, target) in instance
        .argument_list
        .iter()
        .zip(signature.parameter_list.iter())
    {
        let target_colname = &target.stottr_variable.name;
        if original.list_expand {
            to_expand.push(target_colname.clone());
        }
        match &original.term {
            StottrTerm::Variable(v) => {
                if let Some(c) = dynamic_columns.get(&v.name) {
                    existing.push(&v.name);
                    new.push(target_colname);
                    new_dynamic_columns.insert(target_colname.clone(), c.clone());
                } else if let Some(c) = constant_columns.get(&v.name) {
                    new_constant_columns.insert(target_colname.clone(), c.clone());
                } else {
                    return Err(MappingError::UnknownVariableError(v.name.clone()));
                }
            }
            StottrTerm::ConstantTerm(ct) => {
                if original.list_expand {
                    let (expr, primitive_column) = create_dynamic_expression_from_static(target_colname, ct, &target.ptype)?;
                    lf = lf.with_column(expr);
                    new_dynamic_columns.insert(target_colname.clone(), primitive_column);
                    new.push(target_colname);
                } else {
                    let static_column = StaticColumn {
                        constant_term: ct.clone(),
                        ptype: target.ptype.clone(),
                    };
                    new_constant_columns.insert(target_colname.clone(), static_column);
                }

            }
            StottrTerm::List(_) => {
                todo!()
            }
        }
    }

    // TODO: Remove workaround likely bug in Pola.rs 0.25.1
    lf = lf
        .rename(existing.as_slice(), new.as_slice())
        .collect()
        .unwrap()
        .lazy();

    let new_column_expressions: Vec<Expr> = new.into_iter().map(|x| col(&x)).collect();
    lf = lf.select(new_column_expressions.as_slice());

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
        //Todo: List expanders for constant terms..
    }
    Ok((lf, new_dynamic_columns, new_constant_columns))
}
