// Debug utilities for conditional logging based on TANTIVY4JAVA_DEBUG environment variable

use once_cell::sync::Lazy;
use std::env;
use chrono;

/// Global debug flag for tantivy4java, evaluated once at startup
pub static TANTIVY4JAVA_DEBUG_ENABLED: Lazy<bool> = Lazy::new(|| {
    env::var("TANTIVY4JAVA_DEBUG")
        .map(|v| v == "1" || v.to_lowercase() == "true")
        .unwrap_or(false)
});

/// Format timestamp for debug output
pub fn format_timestamp() -> String {
    chrono::Utc::now().format("%H:%M:%S%.3f").to_string()
}

/// Macro for conditional debug printing when TANTIVY4JAVA_DEBUG is enabled
#[macro_export]
macro_rules! tantivy4java_debug {
    ($($arg:tt)*) => {
        if *$crate::debug::TANTIVY4JAVA_DEBUG_ENABLED {
            eprintln!("[{}] {}", $crate::debug::format_timestamp(), format!($($arg)*));
        }
    };
}