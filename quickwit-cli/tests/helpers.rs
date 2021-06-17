
pub fn create_directory() {
    let path = temp_dir();
    create_dir(path.clone()).unwrap();
    path
}

pub fn create_file() {
    let path = temp_dir();
    create_dir(path.clone()).unwrap();
    path
}

/// Creates a file with given content
pub fn create_file(config: &str) -> PathBuf {
    let path = temp_file();
    overwrite_file(path.clone(), config);
    path
}
