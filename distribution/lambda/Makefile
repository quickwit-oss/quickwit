.SILENT:
.ONESHELL:
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c

QW_LAMBDA_VERSION?=beta
PACKAGE_BASE_URL=https://github.com/quickwit-oss/quickwit/releases/download/aws-lambda-$(QW_LAMBDA_VERSION)/
SEARCHER_PACKAGE_FILE=quickwit-lambda-searcher-$(QW_LAMBDA_VERSION)-x86_64.zip
INDEXER_PACKAGE_FILE=quickwit-lambda-indexer-$(QW_LAMBDA_VERSION)-x86_64.zip
export SEARCHER_PACKAGE_PATH=cdk.out/$(SEARCHER_PACKAGE_FILE)
export INDEXER_PACKAGE_PATH=cdk.out/$(INDEXER_PACKAGE_FILE)

check-env:
ifndef CDK_ACCOUNT
	$(error CDK_ACCOUNT is undefined)
endif
ifndef CDK_REGION
	$(error CDK_REGION is undefined)
endif

# Download the packages from the Quickwit release page
# The package varsion can be set with QW_LAMBDA_VERSION
# Build them locally by setting QW_LAMBDA_BUILD=1
package:
	mkdir -p cdk.out
	if [ "$${QW_LAMBDA_BUILD:-0}" = "1" ]
	then
	  cargo lambda build \
	    --manifest-path=../../quickwit/Cargo.toml \
	    -p quickwit-lambda \
	    --release \
	    --output-format zip \
		--target x86_64-unknown-linux-gnu
	  cp -u ../../quickwit/target/lambda/searcher/bootstrap.zip $(SEARCHER_PACKAGE_PATH)
	  cp -u ../../quickwit/target/lambda/indexer/bootstrap.zip $(INDEXER_PACKAGE_PATH)
	else
	  if ! [ -f $(SEARCHER_PACKAGE_PATH) ]; then
	    curl -C - -Ls -o $(SEARCHER_PACKAGE_PATH) $(PACKAGE_BASE_URL)$(SEARCHER_PACKAGE_FILE)
	  fi
	  if ! [ -f $(INDEXER_PACKAGE_PATH) ]; then
	  	curl -C - -Ls -o $(INDEXER_PACKAGE_PATH) $(PACKAGE_BASE_URL)$(INDEXER_PACKAGE_FILE)
	  fi
	fi

indexer-package-path:
	echo -n $(INDEXER_PACKAGE_PATH)

searcher-package-path:
	echo -n $(SEARCHER_PACKAGE_PATH)

bootstrap: package check-env
	cdk bootstrap aws://$$CDK_ACCOUNT/$$CDK_REGION

generate-base-stack:
	cdk synth -a cdk/app.py BaseQuickwitStack

deploy-hdfs: package check-env
	cdk deploy -a cdk/app.py HdfsStack
	
invoke-hdfs-indexer: check-env
	python -c 'from cdk import cli; cli.upload_hdfs_src_file()'
	python -c 'from cdk import cli; cli.invoke_hdfs_indexer()'

invoke-hdfs-searcher-term: check-env
	python -c 'from cdk import cli; cli.invoke_hdfs_searcher("""{"query": "severity_text:ERROR", "max_hits": 10}""")'

invoke-hdfs-searcher-histogram: check-env
	python -c 'from cdk import cli; cli.invoke_hdfs_searcher("""{ "query": "*", "max_hits": 0, "aggs": { "events": { "date_histogram": { "field": "timestamp", "fixed_interval": "1d" }, "aggs": { "log_level": { "terms": { "size": 10, "field": "severity_text", "order": { "_count": "desc" } } } } } } }""")'

deploy-mock-data: package check-env
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
	for mem_size in "$${mem_sizes[@]}"
	do
		export SEARCHER_MEMORY_SIZE=$${mem_size}
		$(MAKE) deploy-hdfs
		python -c 'from cdk import cli; cli.benchmark_hdfs_search("""{"query": "severity_text:ERROR", "max_hits": 10}""")'
	done

bench-search-histogram:
	mem_sizes=( 1024 2048 4096 8192 )
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
		export QW_LAMBDA_PARTIAL_REQUEST_CACHE_CAPACITY=0
		$(MAKE) bench-search-term
		$(MAKE) bench-search-histogram
		export QW_LAMBDA_DISABLE_SEARCH_CACHE=false
		export QW_LAMBDA_PARTIAL_REQUEST_CACHE_CAPACITY=64MB
		$(MAKE) bench-search-term
		$(MAKE) bench-search-histogram
	done


destroy:
	cdk destroy -a cdk/app.py HdfsStack MockDataStack


clean:
	rm -rf cdk.out
