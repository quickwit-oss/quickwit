import aws_cdk
from aws_cdk import aws_lambda, aws_s3
from constructs import Construct


class IndexerService(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        store_bucket: aws_s3.Bucket,
        index_id: str,
        index_config_uri: str,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        handler = aws_lambda.Function(
            self,
            id="quickwit-indexer",
            code=aws_lambda.Code.from_asset("../../quickwit/target/lambda/indexer"),
            runtime=aws_lambda.Runtime.PROVIDED_AL2,
            handler="N/A",
            environment={
                "INDEX_BUCKET": store_bucket.bucket_name,
                "METASTORE_BUCKET": store_bucket.bucket_name,
                "INDEX_ID": index_id,
                "INDEX_CONFIG_URI": index_config_uri,
            },
            timeout=aws_cdk.Duration.seconds(30),
            # reserved_concurrent_executions=1,
            memory_size=1024,
            ephemeral_storage_size=aws_cdk.Size.gibibytes(10),
        )

        store_bucket.grant_read_write(handler)

        aws_cdk.CfnOutput(
            self,
            "indexer-function-name",
            value=handler.function_name,
            export_name="indexer-function-name",
        )
