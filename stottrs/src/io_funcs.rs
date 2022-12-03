use std::fs::create_dir;
use std::path::Path;
use crate::mapping::errors::MappingError;

pub(crate) fn create_folder_if_not_exists(path:&Path) -> Result<(), MappingError> {
    if !path.exists() {
        create_dir(path).map_err(|x|MappingError::FolderCreateIOError(x))?;
    }
    Ok(())
}