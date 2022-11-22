use std::ops::Deref;
use oxrdf::NamedNode;
use oxrdf::vocab::xsd;
use polars::prelude::{concat_lst, Expr, LiteralValue, SpecialEq};
use polars_core::datatypes::{AnyValue, DataType, StructChunked};
use polars_core::prelude::{IntoSeries, Series};
use crate::ast::{ConstantLiteral, ConstantTerm, PType};
use crate::constants::{BLANK_NODE_IRI, NONE_IRI};
use crate::mapping::errors::MappingError;
use crate::mapping::validation_inference::RDFNodeType;

pub fn constant_to_expr(
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