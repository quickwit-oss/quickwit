.SILENT:
.ONESHELL:
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c

check-env:
ifndef CDK_ACCOUNT
	$(error CDK_ACCOUNT is undefined)
endif
ifndef CDK_REGION
	$(error CDK_REGION is undefined)
endif

build:
	cargo lambda build --manifest-path=../../quickwit/Cargo.toml -p quickwit-lambda --release

bootstrap: build check-env
	cdk bootstrap aws://$$CDK_ACCOUNT/$$CDK_REGION

generate-base-stack:
	cdk synth -a cdk/app.py BaseQuickwitStack

deploy-hdfs: build check-env
	cdk deploy -a cdk/app.py HdfsStack
	
invoke-hdfs-indexer: check-env
	python -c 'from cdk import cli; cli.upload_hdfs_src_file()'
	python -c 'from cdk import cli; cli.invoke_hdfs_indexer()'

invoke-hdfs-searcher-term: check-env
	python -c 'from cdk import cli; cli.invoke_hdfs_searcher("""{"query": "severity_text:ERROR", "max_hits": 10}""")'

invoke-hdfs-searcher-histogram: check-env
	python -c 'from cdk import cli; cli.invoke_hdfs_searcher("""{ "query": "*", "max_hits": 0, "aggs": { "events": { "date_histogram": { "field": "timestamp", "fixed_interval": "1d" }, "aggs": { "log_level": { "terms": { "size": 10, "field": "severity_text", "order": { "_count": "desc" } } } } } } }""")'

deploy-mock-data: build check-env
	cdk deploy -a cdk/app.py MockDataStack

invoke-mock-data-searcher: check-env
	python -c 'from cdk import cli; cli.invoke_mock_data_searcher()'

bench-index:
	mem_sizes=( 10240 8192 6144 4096 3072 2048 )
	export QW_LAMBDA_DISABLE_MERGE=true
	for mem_size in "$${mem_sizes[@]}"
	do
		export INDEXER_MEMORY_SIZE=$${mem_size}
		$(MAKE) deploy-hdfs
		python -c 'from cdk import cli; cli.benchmark_hdfs_indexing()'
	done

bench-search-term:
	mem_sizes=( 1024 2048 4096 8192 )
	export QW_LAMBDA_PARTIAL_REQUEST_CACHE_CAPACITY=0
	for mem_size in "$${mem_sizes[@]}"
	do
		export SEARCHER_MEMORY_SIZE=$${mem_size}
		$(MAKE) deploy-hdfs
		python -c 'from cdk import cli; cli.benchmark_hdfs_search("""{"query": "severity_text:ERROR", "max_hits": 10}""")'
	done

bench-search-histogram:
	mem_sizes=( 1024 2048 4096 8192 )
	export QW_LAMBDA_PARTIAL_REQUEST_CACHE_CAPACITY=0
	for mem_size in "$${mem_sizes[@]}"
	do
		export SEARCHER_MEMORY_SIZE=$${mem_size}
		$(MAKE) deploy-hdfs
		python -c 'from cdk import cli; cli.benchmark_hdfs_search("""{ "query": "*", "max_hits": 0, "aggs": { "events": { "date_histogram": { "field": "timestamp", "fixed_interval": "1d" }, "aggs": { "log_level": { "terms": { "size": 10, "field": "severity_text", "order": { "_count": "desc" } } } } } } }""")'
	done


bench-search:
	for run in {1..30}
	do 
		export QW_LAMBDA_DISABLE_SEARCH_CACHE=true
		$(MAKE) bench-search-term
		$(MAKE) bench-search-histogram
		export QW_LAMBDA_DISABLE_SEARCH_CACHE=false
		$(MAKE) bench-search-term
		$(MAKE) bench-search-histogram
	done


destroy:
	cdk destroy -a cdk/app.py HdfsStack MockDataStack
