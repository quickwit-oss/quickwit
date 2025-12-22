use quickwit_config::*;

fn main() {
    let source_config = SourceConfig::sample_for_regression();
    let index_config = IndexConfig::sample_for_regression();
    
    let source_fingerprint = source_config.indexing_params_fingerprint();
    let index_fingerprint = index_config.indexing_params_fingerprint();
    let combined_fingerprint = indexing_pipeline_params_fingerprint(&index_config, &source_config);
    
    println!("Source fingerprint: {}", source_fingerprint);
    println!("Index fingerprint: {}", index_fingerprint);
    println!("Combined fingerprint: {}", combined_fingerprint);
    
    let heap_size = index_config.indexing_settings.resources.heap_size;
    println!("Heap size: {}", heap_size);
    println!("Heap size bytes: {}", heap_size.as_u64());
}