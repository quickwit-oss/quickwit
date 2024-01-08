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

export INDEX_ID=hdfs-logs

build:
	cargo lambda build --manifest-path=../../quickwit/Cargo.toml -p quickwit-lambda --release

init: build check-env
	cdk bootstrap aws://$$CDK_ACCOUNT/$$CDK_REGION

deploy: build check-env
	cdk deploy --parameters quickwitIndexId=$$INDEX_ID
	python -c 'import cli; cli.upload_index_config()'

upload-src-file: check-env
	python -c 'import cli; cli.upload_src_file()'

invoke-indexer: check-env
	python -c 'import cli; cli.invoke_indexer()'

invoke-searcher: check-env
	python -c 'import cli; cli.invoke_searcher()'
