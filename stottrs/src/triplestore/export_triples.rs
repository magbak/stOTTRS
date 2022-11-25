use super::Triplestore;
use oxrdf::{Literal, NamedNode, Subject, Term, Triple};
use polars_core::prelude::{AnyValue};
use crate::triplestore::conversion::convert_to_string;

impl Triplestore {
    pub fn object_property_triples<F, T>(&self, func: F, out: &mut Vec<T>)
    where
        F: Fn(&str, &str, &str) -> T,
    {
        for object_property_triples in &self.get_object_property_triples() {
            if object_property_triples.height() > 0 {
                let mut subject_iterator =
                    object_property_triples.column("subject").unwrap().iter();
                let mut verb_iterator = object_property_triples.column("verb").unwrap().iter();
                let mut object_iterator = object_property_triples.column("object").unwrap().iter();
                for _ in 0..object_property_triples.height() {
                    let s = anyutf8_to_str(subject_iterator.next().unwrap());
                    let v = anyutf8_to_str(verb_iterator.next().unwrap());
                    let o = anyutf8_to_str(object_iterator.next().unwrap());
                    out.push(func(s, v, o));
                }
            }
        }
    }

    pub fn string_data_property_triples<F, T>(&self, func: F, out: &mut Vec<T>)
    where
        F: Fn(&str, &str, &str, Option<&str>) -> T,
    {
        //subject, verb, lexical_form, language_tag, datatype
        for data_property_triples in &self.get_string_property_triples() {
            if data_property_triples.height() > 0 {
                let mut subject_iterator = data_property_triples.column("subject").unwrap().iter();
                let mut verb_iterator = data_property_triples.column("verb").unwrap().iter();
                let mut data_iterator = data_property_triples.column("object").unwrap().iter();
                let mut language_tag_iterator = data_property_triples.column("language_tag").unwrap().iter();
                for _ in 0..data_property_triples.height() {
                    let s = anyutf8_to_str(subject_iterator.next().unwrap());
                    let v = anyutf8_to_str(verb_iterator.next().unwrap());
                    let lex = anyutf8_to_str(data_iterator.next().unwrap());
                    let lang_opt = if let AnyValue::Utf8(lang) = language_tag_iterator.next().unwrap() {
                        Some(lang)
                    } else {
                        None
                    };
                    out.push(func(s, v, lex, lang_opt));
                }
            }
        }
    }

    pub fn nonstring_data_property_triples<F, T>(&self, func: F, out: &mut Vec<T>)
    where
        F: Fn(&str, &str, &str, &NamedNode) -> T,
    {
        //subject, verb, lexical_form, language_tag, datatype
        for (data_property_triples, object_type) in self.get_non_string_property_triples() {
            if data_property_triples.height() > 0 {
                let mut subject_iterator = data_property_triples.column("subject").unwrap().iter();
                let mut verb_iterator = data_property_triples.column("verb").unwrap().iter();
                let data_as_strings = convert_to_string(data_property_triples.column("object").unwrap());
                if let Some(s) = data_as_strings {
                    let mut data_iterator = s.iter();
                    for _ in 0..data_property_triples.height() {
                        let s = anyutf8_to_str(subject_iterator.next().unwrap());
                        let v = anyutf8_to_str(verb_iterator.next().unwrap());
                        let lex = anyutf8_to_str(data_iterator.next().unwrap());
                        out.push(func(s, v, lex, object_type));
                    }
                } else {
                    let mut data_iterator = data_property_triples.column("object").unwrap().iter();
                    for _ in 0..data_property_triples.height() {
                        let s = anyutf8_to_str(subject_iterator.next().unwrap());
                        let v = anyutf8_to_str(verb_iterator.next().unwrap());
                        let lex = anyutf8_to_str(data_iterator.next().unwrap());
                        out.push(func(s, v, lex, object_type));
                    }
                };

            }
        }
    }

    pub fn export_oxrdf_triples(&mut self) -> Vec<Triple> {
        self.deduplicate();
        fn subject_from_str(s: &str) -> Subject {
            Subject::NamedNode(NamedNode::new_unchecked(s))
        }
        fn object_term_from_str(s: &str) -> Term {
            Term::NamedNode(NamedNode::new_unchecked(s))
        }

        fn object_triple_func(s: &str, v: &str, o: &str) -> Triple {
            let subject = subject_from_str(s);
            let verb = NamedNode::new_unchecked(v);
            let object = object_term_from_str(o);
            Triple::new(subject, verb, object)
        }

        fn string_data_triple_func(
            s: &str,
            v: &str,
            lex: &str,
            lang_opt: Option<&str>,
        ) -> Triple {
            let subject = subject_from_str(s);
            let verb = NamedNode::new_unchecked(v);
            let literal = if let Some(lang) = lang_opt {
                Literal::new_language_tagged_literal_unchecked(lex, lang)
            } else {
                Literal::new_simple_literal(lex)
            };
            Triple::new(subject, verb, Term::Literal(literal))
        }

        fn nonstring_data_triple_func(
            s: &str,
            v: &str,
            lex: &str,
            dt: &NamedNode,
        ) -> Triple {
            let subject = subject_from_str(s);
            let verb = NamedNode::new_unchecked(v);
            let literal = Literal::new_typed_literal(lex, dt.clone());
            Triple::new(subject, verb, Term::Literal(literal))
        }

        let mut triples = vec![];
        self.object_property_triples(object_triple_func, &mut triples);
        self.string_data_property_triples(string_data_triple_func, &mut triples);
        self.nonstring_data_property_triples(nonstring_data_triple_func, &mut triples);
        triples
    }
}

fn anyutf8_to_str(a: AnyValue) -> &str {
    if let AnyValue::Utf8(s) = a {
        s
    } else {
        panic!("Should never happen {}", a)
    }
}
