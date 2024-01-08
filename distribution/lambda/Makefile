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

invoke-hdfs-searcher: check-env
	python -c 'from cdk import cli; cli.invoke_hdfs_searcher()'

deploy-mock-data: build check-env
	cdk deploy -a cdk/app.py MockDataStack

invoke-mock-data-searcher: check-env
	python -c 'from cdk import cli; cli.invoke_mock_data_searcher()'

destroy:
	cdk destroy -a cdk/app.py HdfsStack MockDataStack
