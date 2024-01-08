.SILENT:
.ONESHELL:

check-env:
ifndef CDK_ACCOUNT
	$(error CDK_ACCOUNT is undefined)
endif
ifndef CDK_REGION
	$(error CDK_REGION is undefined)
endif

build:
	cargo lambda build --manifest-path=../../quickwit/Cargo.toml -p quickwit-lambda --release
	cp resources/indexer-config.yaml ../../quickwit/target/lambda/indexer/config.yaml
	cp resources/searcher-config.yaml ../../quickwit/target/lambda/searcher/config.yaml

init: build check-env
	cdk bootstrap aws://$$CDK_ACCOUNT/$$CDK_REGION

deploy: build check-env
	cdk deploy

export EXAMPLE_FILE=hdfs-logs-multitenants-10000.json

upload-src-file: check-env
	export AWS_REGION=$$CDK_REGION
	bucket_name=$$(aws cloudformation describe-stacks --stack-name LambdaStack --query "Stacks[0].Outputs[?ExportName=='index-store-bucket-name'].OutputValue" --output text)
	source_uri="s3://$$bucket_name/$$EXAMPLE_FILE"
	echo "upload src file to $$source_uri"
	curl https://quickwit-datasets-public.s3.amazonaws.com/$$EXAMPLE_FILE | aws s3 cp - $$source_uri

invoke-indexer: check-env
	export AWS_REGION=$$CDK_REGION
	function_name=$$(aws cloudformation describe-stacks --stack-name LambdaStack --query "Stacks[0].Outputs[?ExportName=='indexer-function-name'].OutputValue" --output text)
	bucket_name=$$(aws cloudformation describe-stacks --stack-name LambdaStack --query "Stacks[0].Outputs[?ExportName=='index-store-bucket-name'].OutputValue" --output text)	
	source_uri="s3://$$bucket_name/$$EXAMPLE_FILE"
	echo "indexer function name: $$function_name, src_file: $$source_uri"
	aws lambda invoke \
		--cli-binary-format raw-in-base64-out \
		--payload "{ \"source_uri\": \"$$source_uri\" }" \
		--function-name $$function_name \
		--log-type Tail /dev/null \
		| jq -r '.LogResult' \
		| base64 -d

invoke-searcher: check-env
	export AWS_REGION=$$CDK_REGION
	function_name=$$(aws cloudformation describe-stacks --stack-name LambdaStack --query "Stacks[0].Outputs[?ExportName=='searcher-function-name'].OutputValue" --output text)
	echo "searcher function name: $$function_name"
	aws lambda invoke --function-name $$function_name --log-type Tail /dev/null | jq -r '.LogResult' | base64 -d


index-creation-instruction: deploy
	export AWS_REGION=$$CDK_REGION
	BUCKET_NAME=$$(aws cloudformation describe-stacks --stack-name LambdaStack --query "Stacks[0].Outputs[?ExportName=='index-store-bucket-name'].OutputValue" --output text)
	echo "\n=> start an interactive quickwit shell"
	echo "docker run --entrypoint bash -it quickwit/quickwit"
	echo "\n=> inside the interactive shell, configure the AWS credentials"
	echo "export AWS_ACCESS_KEY_ID=..."
	echo "export AWS_SECRET_ACCESS_KEY=..."
	echo "export AWS_SESSION_TOKEN=..."
	echo "\n=> then configure and start the server"
	echo "apt update && apt install curl -y"
	echo "curl -o hdfs_logs_index_config.yaml https://raw.githubusercontent.com/quickwit-oss/quickwit/main/config/tutorials/hdfs-logs/index-config.yaml"
	echo "export AWS_REGION=$$CDK_REGION"
	echo "echo \"metastore_uri: s3://$$BUCKET_NAME\" >> config/quickwit.yaml"
	echo "echo \"default_index_root_uri: s3://$$BUCKET_NAME\" >> config/quickwit.yaml"
	echo "quickwit run &"
	echo "\n=> once the server has started, create the index"
	echo "quickwit index create --index-config hdfs_logs_index_config.yaml"
