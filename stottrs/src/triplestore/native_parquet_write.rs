use std::fs::{create_dir, File};
use super::Triplestore;
use nom::InputIter;
use std::path::Path;
use std::time::Instant;
use log::debug;
use polars::prelude::ParquetWriter;
use polars_core::frame::DataFrame;
use rayon::iter::ParallelDrainRange;
use rayon::iter::ParallelIterator;
use crate::mapping::errors::MappingError;
use crate::mapping::RDFNodeType;

impl Triplestore {
    pub fn write_native_parquet(&mut self, path: &Path) -> Result<(), MappingError>{
        let now = Instant::now();
        if !path.exists() {
            return Err(MappingError::PathDoesNotExist(path.to_str().unwrap().to_string()))
        }
        let path_buf = path.to_path_buf();

        self.deduplicate();

        let mut dfs_to_write = vec![];

        for (property, tts) in &mut self.df_map {
            for (rdf_node_type, tt) in tts {
                let filename;
                if let RDFNodeType::Literal(literal_type) = rdf_node_type {
                    filename = format!(
                        "{}_{}",
                        property_to_filename(property),
                        property_to_filename(literal_type.as_str())
                    );
                } else {
                    filename = format!(
                        "{}_object_property",
                        property_to_filename(property),
                    )
                }
                let mut file_path = path_buf.clone();
                for (i, df) in tt.dfs.iter_mut().enumerate() {
                    let filename = format!("{filename}_part_{i}.parquet");
                    let mut file_path = file_path.clone();
                    file_path.push(filename);
                    dfs_to_write.push((df, file_path));
                }
            }
        }

        let results:Vec<Result<(), MappingError>> = dfs_to_write.par_drain(..).map(|(df, file_path)|write_parquet(df, file_path.as_path())).collect();
        for r in results {
            r?;
        }

        debug!("Writing native parquet took {} seconds", now.elapsed().as_secs_f64());
        Ok(())
    }
}

fn property_to_filename(property_name: &str) -> String {
    property_name
        .iter_elements()
        .filter(|x| x.is_alphanumeric())
        .collect()
}

fn write_parquet(df:&mut DataFrame, file_path:&Path) -> Result<(), MappingError> {
    let file = File::create(file_path).map_err(|x|MappingError::FileCreateIOError(x))?;
    let mut writer = ParquetWriter::new(file);
    writer = writer.with_row_group_size(Some(1_000));
    writer.finish(df).map_err(|x|MappingError::WriteParquetError(x))?;
    Ok(())
}

fn create_folder_if_not_exists(path:&Path) -> Result<(), MappingError> {
    if !path.exists() {
        create_dir(path).map_err(|x|MappingError::FolderCreateIOError(x))?;
    }
    Ok(())
}