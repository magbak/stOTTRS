use std::fs::File;
use std::path::Path;
use nom::InputIter;
use polars::prelude::ParquetWriter;
use polars_core::frame::DataFrame;
use crate::mapping::errors::MappingError;

pub(crate) fn property_to_filename(property_name: &str) -> String {
    property_name
        .iter_elements()
        .filter(|x| x.is_alphanumeric())
        .collect()
}

pub(crate) fn write_parquet(df:&mut DataFrame, file_path:&Path) -> Result<(), MappingError> {
    let file = File::create(file_path).map_err(|x|MappingError::FileCreateIOError(x))?;
    let mut writer = ParquetWriter::new(file);
    writer = writer.with_row_group_size(Some(1_000));
    writer.finish(df).map_err(|x|MappingError::WriteParquetError(x))?;
    Ok(())
}