fn main() {
    let git_commit_hash = std::env::var_os("GITHUB_SHA")
        .map_or("Unknown".to_string(), |value_os_str| {
            value_os_str.to_string_lossy().to_string()
        });
    println!("cargo:rustc-env=GIT_COMMIT_HASH={}", git_commit_hash);
}
