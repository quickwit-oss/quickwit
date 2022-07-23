mod access_log;
mod cache;
mod file;
mod store;

pub(crate) use cache::DiskBackedLRUCache;

fn time_now() -> std::time::Duration {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
}
