use binggan::plugins::PeakMemAllocPlugin;
use binggan::{INSTRUMENTED_SYSTEM, InputGroup, PeakMemAlloc, black_box};
use quickwit_processing::transformers::SyslogProcessor;
use quickwit_processing::{
    DatadogLogMsg, PipelineStep, ProcessedLog, get_integrations_processor,
    get_preprocessing_pipeline,
};
use rand::Rng;
use time::OffsetDateTime;

#[global_allocator]
pub static GLOBAL: &PeakMemAlloc<std::alloc::System> = &INSTRUMENTED_SYSTEM;

fn make_test_processed_log(message: String) -> ProcessedLog {
    let datadog_msg = DatadogLogMsg {
        message: message.into(),
        status: Some("info".to_string()),
        timestamp: Some(OffsetDateTime::now_utc()),
        hostname: Some("test-host".to_string()),
        service: Some("test-service".to_string()),
        ddsource: Some("syslog".to_string()),
        ddtags: vec!["env:test".to_string()],
    };
    ProcessedLog::from_datadog_log_msg(datadog_msg)
}

fn generate_syslog_processed_logs(count: usize) -> Vec<ProcessedLog> {
    let mut rng = rand::thread_rng();
    let mut processed_logs = Vec::with_capacity(count);

    for _ in 0..count {
        // Randomize various parts of the message
        let prival = rng.gen_range(0..192); // 0-191 are valid syslog priorities
        let hostname = "mymachine.example.com";
        let app_names = [
            "nginx_access",
            "vcap_nginx_access",
            "apache_access",
            "haproxy",
            "sshd",
            "kernel",
        ];
        let app_name = app_names[rng.gen_range(0..app_names.len())];
        let methods = ["GET", "POST", "PUT", "DELETE", "HEAD"];
        let paths = [
            "/healthz",
            "/api/v1/status",
            "/metrics",
            "/",
            "/login",
            "/dashboard",
        ];
        let method = methods[rng.gen_range(0..methods.len())];
        let path = paths[rng.gen_range(0..paths.len())];
        let status_code = 200;
        let response_size = 1024;
        let ip = "192.168.1.100";
        let response_time = 0.125;
        let request_id = "12345678-abcd-ef01-2345-123456789abc";

        let message = format!(
            r#"<{prival}>1 2025-10-14T07:10:44+00:00 {hostname} {app_name} - - -  {ip} - [14/Oct/2025:07:10:44 +0000] "{method} {path} HTTP/1.1" {status_code} {response_size} "-" "curl/7.81.0" 127.0.0.1 vcap_request_id:{request_id} response_time:{response_time:.3}"#,
            prival = prival,
            hostname = hostname,
            app_name = app_name,
            ip = ip,
            method = method,
            path = path,
            status_code = status_code,
            response_size = response_size,
            request_id = request_id,
            response_time = response_time
        );
        processed_logs.push(make_test_processed_log(message));
    }

    processed_logs
}

fn generate_go_processed_logs(count: usize) -> Vec<ProcessedLog> {
    let mut rng = rand::thread_rng();
    let mut processed_logs = Vec::with_capacity(count);

    for _ in 0..count {
        // Generate different types of Go log messages based on the integration patterns
        let log_type = rng.gen_range(0..3);

        let message = match log_type {
            0 => {
                // go_prefixed pattern: [Oct 27 02:18:00] DEBUG main: started observing beach
                // animal=walrus
                let levels = ["DEBUG", "INFO", "WARN", "ERROR"];
                let level = levels[rng.gen_range(0..levels.len())];
                let thread_names = ["main", "worker", "handler", "scheduler"];
                let thread_name = thread_names[rng.gen_range(0..thread_names.len())];
                let messages = [
                    "started observing beach animal=walrus",
                    "processing request id=12345 user=john",
                    "connection established host=localhost port=8080",
                    "task completed duration=1.5s status=success",
                ];
                let msg = messages[rng.gen_range(0..messages.len())];

                format!("[Oct 27 02:18:00] {} {}: {}", level, thread_name, msg)
            }
            1 => {
                // go_default pattern: 2017/03/02 16:07:16 You cannot divide by 0
                let error_messages = [
                    "You cannot divide by 0",
                    "Connection timeout after 30s",
                    "Invalid configuration parameter",
                    "Database connection failed",
                    "Authentication token expired",
                ];
                let error_msg = error_messages[rng.gen_range(0..error_messages.len())];

                format!("2017/03/02 16:07:16 {}", error_msg)
            }
            _ => {
                // go_fallback keyvalue pattern: timestamp="2015-03-26T01:27:38-04:00" level=debug
                // msg="Started observing beach" animal=walrus number=8
                let levels = ["debug", "info", "warn", "error"];
                let level = levels[rng.gen_range(0..levels.len())];
                let messages = [
                    "Started observing beach",
                    "Processing user request",
                    "Database query executed",
                    "Cache miss occurred",
                ];
                let msg = messages[rng.gen_range(0..messages.len())];
                let animals = ["walrus", "seal", "penguin", "dolphin"];
                let animal = animals[rng.gen_range(0..animals.len())];
                let number = rng.gen_range(1..100);

                format!(
                    r#"timestamp="2015-03-26T01:27:38-04:00" level={} msg="{}" animal={} number={}"#,
                    level, msg, animal, number
                )
            }
        };

        // Create ProcessedLog with Go source
        let mut processed_log = make_test_processed_log(message);
        processed_log.source = Some("go".to_string());
        processed_logs.push(processed_log);
    }

    processed_logs
}

fn bench_syslog_processor_parsing(mut runner: InputGroup<Vec<ProcessedLog>, u64>) {
    runner.add_plugin(PeakMemAllocPlugin::new(GLOBAL));

    runner.throughput(|processed_logs| {
        processed_logs
            .iter()
            .map(|log| log.message.len())
            .sum::<usize>()
    });

    runner.register("syslog_processor", |processed_logs| {
        let processor = SyslogProcessor;
        let mut total_processed = 0u64;

        for mut processed_log in processed_logs.iter().cloned() {
            // Benchmark the syslog processing
            processor.apply(&mut processed_log).unwrap();
            total_processed += 1;
            // Ensure the processor actually did work by checking if syslog data exists
            if processed_log.custom.contains_key("syslog") {
                total_processed += processed_log.custom.len() as u64;
            }

            // Prevent optimization
            black_box(&processed_log);
        }

        black_box(total_processed)
    });

    let pipeline = get_preprocessing_pipeline();
    runner.register("preprocessing_pipeline", |processed_logs| {
        let mut total_processed = 0u64;

        for mut processed_log in processed_logs.iter().cloned() {
            // Benchmark the full preprocessing pipeline
            pipeline.apply(&mut processed_log).unwrap();
            total_processed += 1;
            // Ensure the processor actually did work by checking if syslog data exists
            if processed_log.custom.contains_key("syslog") {
                total_processed += processed_log.custom.len() as u64;
            }
            // Prevent optimization
            black_box(&processed_log);
        }

        black_box(total_processed)
    });

    runner.run();
}

fn bench_go_integration_parsing(mut runner: InputGroup<Vec<ProcessedLog>, u64>) {
    runner.throughput(|processed_logs| {
        processed_logs
            .iter()
            .map(|log| log.message.len())
            .sum::<usize>()
    });

    let integrations_processor = get_integrations_processor();
    runner.register("go_integration_processor", |processed_logs| {
        let mut total_processed = 0u64;

        for mut processed_log in processed_logs.iter().cloned() {
            // Benchmark the Go integration processing (includes Grok parsing)
            integrations_processor.apply(&mut processed_log).unwrap();
            total_processed += 1;
            // Count custom fields as a measure of work done (Grok extracts fields)
            total_processed += processed_log.custom.len() as u64;

            // Prevent optimization
            black_box(&processed_log);
        }

        black_box(total_processed)
    });

    runner.run();
}

fn main() {
    let syslog_datasets = vec![(
        "10k syslog messages",
        generate_syslog_processed_logs(10_000),
    )];

    bench_syslog_processor_parsing(InputGroup::new_with_inputs(syslog_datasets));

    let go_datasets = vec![("10k Go messages", generate_go_processed_logs(10_000))];

    bench_go_integration_parsing(InputGroup::new_with_inputs(go_datasets));
}
