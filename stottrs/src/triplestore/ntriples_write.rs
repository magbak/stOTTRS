//Based on writer.rs: https://raw.githubusercontent.com/pola-rs/polars/master/polars/polars-io/src/csv_core/write.rs
//in Pola.rs with license:
//Copyright (c) 2020 Ritchie Vink
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
use polars::error::PolarsError;
use polars::export::rayon::iter::{IntoParallelIterator, ParallelIterator};
use polars::export::rayon::prelude::ParallelExtend;
use polars::prelude::{AnyValue, DataFrame, Series};
use polars::series::SeriesIter;
use polars_core::POOL;
use polars_utils::contention_pool::LowContentionPool;
use std::io::Write;
use oxrdf::NamedNode;
use crate::triplestore::conversion::convert_to_string;
use crate::triplestore::TripleType;
use super::Triplestore;

/// Utility to write to `&mut Vec<u8>` buffer
struct StringWrap<'a>(pub &'a mut Vec<u8>);

impl<'a> std::fmt::Write for StringWrap<'a> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.0.extend_from_slice(s.as_bytes());
        Ok(())
    }
}

impl Triplestore {
    pub(crate) fn write_n_triples_all_dfs<W: Write + ?Sized>(
        &mut self,
        writer: &mut W,
        chunk_size: usize,
    ) -> Result<()> {
        let n_threads = POOL.current_num_threads();
        let mut any_value_iter_pool = LowContentionPool::<Vec<_>>::new(n_threads);
        let mut write_buffer_pool = LowContentionPool::<Vec<_>>::new(n_threads);

        for df in &mut self.get_mut_object_property_triples() {
            df.as_single_chunk_par();
            write_ntriples_for_df(df, &None, writer, chunk_size, TripleType::ObjectProperty, n_threads, &mut any_value_iter_pool, &mut write_buffer_pool)?;
        }
        for df in &mut self.get_mut_string_property_triples() {
            df.as_single_chunk_par();
            write_ntriples_for_df(df, &None, writer, chunk_size, TripleType::StringProperty, n_threads, &mut any_value_iter_pool, &mut write_buffer_pool)?;
        }
        for (df,dt) in &mut self.get_mut_non_string_property_triples() {
            df.as_single_chunk_par();
            write_ntriples_for_df(df, &Some(dt.clone()), writer, chunk_size, TripleType::NonStringProperty, n_threads, &mut any_value_iter_pool, &mut write_buffer_pool)?;
        }

        Ok(())
    }
}

fn write_ntriples_for_df<W: Write + ?Sized>(
    df: &DataFrame,
    dt: &Option<NamedNode>,
    writer: &mut W,
    chunk_size: usize,
    triple_type: TripleType,
    n_threads: usize,
    any_value_iter_pool: &mut LowContentionPool<Vec<SeriesIter>>,
    write_buffer_pool: &mut LowContentionPool<Vec<u8>>
) -> Result<()>{
        let dt_str = if triple_type == TripleType::NonStringProperty {
            if let Some(nn) = dt {
                Some(nn.as_str())
            } else {
                panic!("Must have datatype for non string property")
            }
        } else {
            None
        };

        let len = df.height();

        let total_rows_per_pool_iter = n_threads * chunk_size;

        let mut n_rows_finished = 0;

        // holds the buffers that will be written
        let mut result_buf = Vec::with_capacity(n_threads);
        while n_rows_finished < len {
            let par_iter = (0..n_threads).into_par_iter().map(|thread_no| {
                let thread_offset = thread_no * chunk_size;
                let total_offset = n_rows_finished + thread_offset;
                let mut df = df.slice(total_offset as i64, chunk_size);
                //We force all objects to string-representations here
                if let Some(s) = convert_to_string(df.column("object").unwrap()) {
                    df.with_column(s).unwrap();
                }

                let cols = df.get_columns();

                // Safety:
                // the bck thinks the lifetime is bounded to write_buffer_pool, but at the time we return
                // the vectors the buffer pool, the series have already been removed from the buffers
                // in other words, the lifetime does not leave this scope
                let cols = unsafe { std::mem::transmute::<&Vec<Series>, &Vec<Series>>(cols) };
                let mut write_buffer = write_buffer_pool.get();

                // don't use df.empty, won't work if there are columns.
                if df.height() == 0 {
                    return write_buffer;
                }

                let any_value_iters = cols.iter().map(|s| s.iter());
                let mut col_iters = any_value_iter_pool.get();
                col_iters.extend(any_value_iters);

                let mut finished = false;
                // loop rows
                while !finished {
                    let mut any_values = vec![];
                    for col in &mut col_iters {
                        match col.next() {
                            Some(value) => {
                                any_values.push(value)
                            }
                            None => {
                                finished = true;
                                break;
                            }
                        }
                    }
                    if !any_values.is_empty() {
                        match triple_type {
                            TripleType::ObjectProperty => {
                                write_object_property_triple(&mut write_buffer, any_values);
                            }
                            TripleType::StringProperty => {
                                write_string_property_triple(&mut write_buffer, any_values);
                            }
                            TripleType::NonStringProperty => {
                                write_non_string_property_triple(&mut write_buffer, dt_str.unwrap(), any_values);
                            }
                        }
                    }
                }

                // return buffers to the pool
                col_iters.clear();
                any_value_iter_pool.set(col_iters);

                write_buffer
            });
            // rayon will ensure the right order
            result_buf.par_extend(par_iter);

            for mut buf in result_buf.drain(..) {
                let _ = writer.write(&buf)?;
                buf.clear();
                write_buffer_pool.set(buf);
            }

            n_rows_finished += total_rows_per_pool_iter;
        }
    Ok(())
}

fn write_string_property_triple(f: &mut Vec<u8>, mut any_values: Vec<AnyValue>) {
    let lang_opt = if let AnyValue::Utf8(lang) = any_values.pop().unwrap() {Some(lang)} else {None};
    let lex = if let AnyValue::Utf8(lex) = any_values.pop().unwrap() {lex} else {panic!()};
    let v = if let AnyValue::Utf8(v) = any_values.pop().unwrap() {v} else {panic!()};
    let s = if let AnyValue::Utf8(s) = any_values.pop().unwrap() {s} else {panic!()};
    write!(f, "<{}>", s).unwrap();
    write!(f, " <{}>", v).unwrap();
    write!(f, " \"{}\"", lex).unwrap();
    if let Some(lang) = lang_opt {
        writeln!(f, "@{} .", lang).unwrap();
    } else {
        writeln!(f, " .").unwrap();
    }
}

//Assumes that the data has been bulk-converted
fn write_non_string_property_triple(f: &mut Vec<u8>, dt:&str, mut any_values: Vec<AnyValue>) {
    println!("Anyvalues {:?}", any_values);

    let lex = if let AnyValue::Utf8(lex) = any_values.pop().unwrap() {lex} else {panic!()};
    let v = if let AnyValue::Utf8(v) = any_values.pop().unwrap() {v} else {panic!()};
    let s = if let AnyValue::Utf8(s) = any_values.pop().unwrap() {s} else {panic!()};
    write!(f, "<{}>", s).unwrap();
    write!(f, " <{}>", v).unwrap();
    write!(f, " \"{}\"", lex).unwrap();
    writeln!(f, "^^<{}> .", dt).unwrap();
}

fn write_object_property_triple(f: &mut Vec<u8>, mut any_values: Vec<AnyValue>) {
    let o = if let AnyValue::Utf8(o) = any_values.pop().unwrap() {o} else {panic!()};
    let v = if let AnyValue::Utf8(v) = any_values.pop().unwrap() {v} else {panic!()};
    let s = if let AnyValue::Utf8(s) = any_values.pop().unwrap() {s} else {panic!()};
    write!(f, "<{}>", s).unwrap();
    write!(f, " <{}>", v).unwrap();
    writeln!(f, " <{}> .", o).unwrap();
}

pub type Result<T> = std::result::Result<T, PolarsError>;
