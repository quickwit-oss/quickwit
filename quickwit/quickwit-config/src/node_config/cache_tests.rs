// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;

#[test]
fn test_cache_defaults_and_capacity_aliases() {
    let defaults = SearcherConfig::default();
    assert!(defaults.disk_cache.is_none());
    assert!(defaults.fast_field_cache.is_none());
    assert_eq!(
        defaults.resolved_fast_field_cache().capacity(),
        ByteSize::gb(1)
    );
    for yaml in [
        "{}",
        "partial_request_cache_capacity: 64M\npredicate_cache_capacity: 256M",
        "partial_request_cache: 64M\npredicate_cache: 256M",
        "partial_request_cache: {memory_capacity: 64M}\npredicate_cache: {capacity: 256M, \
         virtual_caches: []}",
    ] {
        let config: SearcherConfig = serde_yaml::from_str(yaml).unwrap();
        config.validate().unwrap();
        assert_eq!(config.partial_request_cache.capacity(), ByteSize::mb(64));
        assert_eq!(config.predicate_cache.capacity(), ByteSize::mb(256));
        assert!(
            config
                .partial_request_cache
                .disk_cache(None)
                .unwrap()
                .is_none()
        );
        assert!(config.predicate_cache.disk_cache(None).unwrap().is_none());
    }
    for yaml in [
        "fast_field_cache_capacity: 32M",
        "fast_field_cache: 32M",
        "fast_field_cache: {capacity: 32M}",
        "fast_field_cache: {memory_capacity: 32M}",
    ] {
        let config: SearcherConfig = serde_yaml::from_str(yaml).unwrap();
        config.validate().unwrap();
        assert_eq!(
            config.resolved_fast_field_cache().capacity(),
            ByteSize::mb(32)
        );
    }
}

#[test]
fn test_zero_memory_capacity_disables_caches_without_disk_settings() {
    for yaml in [
        "partial_request_cache: 0\npredicate_cache: {capacity: 0}\nfast_field_cache: 0",
        "partial_request_cache_capacity: 0\npredicate_cache_capacity: \
         0\nfast_field_cache_capacity: 0",
    ] {
        let config: SearcherConfig = serde_yaml::from_str(yaml).unwrap();
        config.validate().unwrap();
        for cache in [&config.partial_request_cache, &config.predicate_cache] {
            assert_eq!(cache.capacity(), ByteSize::b(0));
            assert!(cache.disk_cache(None).unwrap().is_none());
        }
        assert_eq!(
            config.resolved_fast_field_cache().capacity(),
            ByteSize::b(0)
        );
    }
}

#[test]
fn test_disk_cache_changes_only_omitted_fast_field_default() {
    let yaml = "disk_cache: {path: /tmp/split, disk_capacity: 300GiB, memory_capacity: 1GiB}";
    let mut config: SearcherConfig = serde_yaml::from_str(yaml).unwrap();
    config.validate().unwrap();
    assert_eq!(config.resolved_fast_field_cache(), CacheConfig::no_cache());
    config.fast_field_cache = Some(CacheConfig::default_with_capacity(ByteSize::gb(1)));
    assert_eq!(
        config.resolved_fast_field_cache().capacity(),
        ByteSize::gb(1)
    );
    let config: SearcherConfig = serde_yaml::from_str("disk_cache: null").unwrap();
    assert!(config.disk_cache.is_none());
    assert_eq!(
        config.resolved_fast_field_cache().capacity(),
        ByteSize::gb(1)
    );
}

#[test]
fn test_shared_directory_resolution_and_overrides() {
    let mut config: SearcherConfig = serde_yaml::from_str(
        "disk_cache_directory: /tmp/feuer\ndisk_cache: {disk_capacity: 300GiB, memory_capacity: \
         1GiB}\npartial_request_cache: {disk_capacity: 20GiB}\npredicate_cache: {disk_capacity: 0}",
    )
    .unwrap();
    config.validate().unwrap();
    let directory = config.disk_cache_directory.as_deref();
    let partial = config
        .partial_request_cache
        .disk_cache(directory)
        .unwrap()
        .unwrap();
    let predicate = config
        .predicate_cache
        .disk_cache(directory)
        .unwrap()
        .unwrap();
    assert_eq!(partial.memory_capacity, ByteSize::mb(64));
    assert_eq!(partial.disk_capacity, ByteSize::gib(20));
    assert_eq!(predicate.memory_capacity, ByteSize::mb(256));
    assert_eq!(predicate.disk_capacity, ByteSize::b(0));
    for cache in [
        config.resolved_disk_cache().unwrap().unwrap(),
        partial,
        predicate,
    ] {
        assert_eq!(cache.directory().unwrap(), Path::new("/tmp/feuer"));
    }
    // Resolution leaves the user-specified configuration unchanged.
    let serialized = serde_json::to_value(&config).unwrap();
    assert!(serialized["disk_cache"].get("path").is_none());
    assert_eq!(serialized["disk_cache_directory"], "/tmp/feuer");
    config.disk_cache.as_mut().unwrap().path = Some(PathBuf::from("/tmp/split"));
    config.predicate_cache.path = Some(PathBuf::from("/tmp/predicate"));
    config.validate().unwrap();
    assert_eq!(
        config
            .resolved_disk_cache()
            .unwrap()
            .unwrap()
            .directory()
            .unwrap(),
        Path::new("/tmp/split")
    );
    assert_eq!(
        config
            .predicate_cache
            .disk_cache(config.disk_cache_directory.as_deref())
            .unwrap()
            .unwrap()
            .directory()
            .unwrap(),
        Path::new("/tmp/predicate")
    );
}

#[test]
fn test_directory_alone_keeps_memory_only_defaults() {
    let config: SearcherConfig = serde_yaml::from_str("disk_cache_directory: /tmp/feuer").unwrap();
    config.validate().unwrap();
    assert!(config.resolved_disk_cache().unwrap().is_none());
    for cache in [&config.partial_request_cache, &config.predicate_cache] {
        assert!(
            cache
                .disk_cache(config.disk_cache_directory.as_deref())
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(
        config.resolved_fast_field_cache().capacity(),
        ByteSize::gb(1)
    );
}

#[test]
fn test_cache_capacities_round_trip_exactly() {
    let cache: CacheConfig =
        serde_yaml::from_str("{capacity: 64000001, path: /tmp/cache, disk_capacity: 1000000001}")
            .unwrap();
    let serialized = serde_json::to_value(&cache).unwrap();
    assert_eq!(serialized["memory_capacity"], 64000001u64);
    assert_eq!(serialized["disk_capacity"], 1000000001u64);
    assert!(serialized.get("capacity").is_none());
    assert_eq!(
        serde_json::from_value::<CacheConfig>(serialized).unwrap(),
        cache
    );
    assert!(
        serde_yaml::from_str::<CacheConfig>("{capacity: 64MiB, memory_capacity: 128MiB}").is_err()
    );
    let cache: DiskCacheConfig =
        serde_yaml::from_str("path: /tmp/split\ndisk_capacity: 300GiB\nmemory_capacity: 1GiB")
            .unwrap();
    cache.validate().unwrap();
    assert_eq!(cache.disk_capacity, ByteSize::gib(300));
    assert_eq!(cache.memory_capacity, ByteSize::gib(1));
    assert_eq!(
        serde_yaml::from_str::<DiskCacheConfig>(&serde_yaml::to_string(&cache).unwrap()).unwrap(),
        cache
    );
}

#[test]
fn test_feuer_rejects_legacy_eviction_settings_even_when_disabled() {
    for name in [
        "partial_request_cache",
        "predicate_cache",
        "fast_field_cache",
    ] {
        for (settings, message) in [
            ("policy: lru", "policy is not supported"),
            ("policy: s3-fifo", "policy is not supported"),
            ("policy: tiny-lfu", "policy is not supported"),
            ("virtual_caches: [{}]", "virtual_caches are not supported"),
        ] {
            for capacity in ["0", "64M"] {
                let config: SearcherConfig = serde_yaml::from_str(&format!(
                    "{name}: {{memory_capacity: {capacity}, {settings}}}",
                ))
                .unwrap();
                let error = format!("{:#}", config.validate().unwrap_err());
                assert!(error.contains(name), "{error}");
                assert!(error.contains(message), "{error}");
            }
        }
    }
}

#[test]
fn test_result_cache_rejects_invalid_disk_settings() {
    for name in ["partial_request_cache", "predicate_cache"] {
        for (settings, message) in [
            ("path: /tmp/cache", "disk_capacity must be set"),
            (
                "disk_capacity: 1G",
                "path or disk_cache_directory must be set",
            ),
            ("path: '', disk_capacity: 1G", "path must not be empty"),
            (
                "path: /tmp/cache, disk_capacity: 0, capacity: 0",
                "capacity must be positive",
            ),
            (
                "path: /tmp/cache, disk_capacity: 1G, policy: lru",
                "policy is not supported",
            ),
            (
                "path: /tmp/cache, disk_capacity: 1G, virtual_caches: [{}]",
                "virtual_caches are not supported",
            ),
            (
                "virtual_caches: [{path: /tmp/cache, disk_capacity: 1G}]",
                "virtual_caches are not supported",
            ),
        ] {
            let config: SearcherConfig =
                serde_yaml::from_str(&format!("{name}: {{{settings}}}")).unwrap();
            let error = format!("{:#}", config.validate().unwrap_err());
            assert!(error.contains(name), "{error}");
            assert!(error.contains(message), "{error}");
        }
    }
}

#[test]
fn test_disk_cache_rejects_missing_or_empty_directory() {
    for yaml in [
        "disk_cache_directory: ''",
        "disk_cache: {disk_capacity: 1GiB, memory_capacity: 1MiB}",
        "disk_cache_directory: /tmp/feuer\ndisk_cache: {path: '', disk_capacity: 1GiB, \
         memory_capacity: 1MiB}",
        "disk_cache_directory: /tmp/feuer\npartial_request_cache: {path: '', disk_capacity: 1GiB}",
    ] {
        let config: SearcherConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.validate().is_err(), "{yaml}");
    }
}

#[test]
fn test_fast_field_cache_rejects_disk_settings() {
    for settings in [
        "path: /tmp/cache",
        "disk_capacity: 0",
        "path: /tmp/cache, disk_capacity: 1G",
    ] {
        let config: SearcherConfig =
            serde_yaml::from_str(&format!("fast_field_cache: {{{settings}}}")).unwrap();
        let error = format!("{:#}", config.validate().unwrap_err());
        assert!(error.contains("invalid fast_field_cache"), "{error}");
        assert!(
            error.contains(
                "path and disk_capacity are only supported by partial_request_cache and \
                 predicate_cache"
            ),
            "{error}"
        );
    }
}

#[test]
fn test_removed_footer_cache_settings_are_rejected() {
    for name in ["split_footer_cache", "split_footer_cache_capacity"] {
        let error = serde_yaml::from_str::<SearcherConfig>(&format!("{name}: 500M")).unwrap_err();
        assert!(
            error
                .to_string()
                .contains(&format!("unknown field `{name}`"))
        );
    }
}

#[test]
fn test_disk_cache_rejects_engine_settings() {
    for setting in [
        "compression: lz4",
        "buffer_pool_size: 512M",
        "submit_queue_size_threshold: 1G",
        "memory_eviction_policy: s3-fifo",
        "write_throughput: 500MiB",
    ] {
        let yaml =
            format!("path: /tmp/split\ndisk_capacity: 300GiB\nmemory_capacity: 1GiB\n{setting}");
        let error = serde_yaml::from_str::<DiskCacheConfig>(&yaml).unwrap_err();
        assert!(
            error
                .to_string()
                .contains(setting.split(':').next().unwrap())
        );
    }
}

#[test]
fn test_disk_cache_accepts_zero_and_unaligned_disk_capacity() {
    for capacity in [
        ByteSize::b(0),
        ByteSize::gb(300),
        ByteSize::b((1 << 20) + 1),
    ] {
        let mut config = DiskCacheConfig::for_test();
        config.disk_capacity = capacity;
        config.validate().unwrap();
        assert_eq!(config.disk_capacity, capacity);
    }
}

#[test]
fn test_disk_cache_rejects_zero_memory_capacity() {
    for capacity in [ByteSize::b(0), ByteSize::mib(1)] {
        let mut config = DiskCacheConfig::for_test();
        config.disk_capacity = capacity;
        config.memory_capacity = ByteSize::b(0);
        assert_eq!(
            config.validate().unwrap_err().to_string(),
            "disk_cache.memory_capacity must be positive"
        );
    }
}
