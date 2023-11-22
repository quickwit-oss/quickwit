import aws_cdk
from aws_cdk import aws_lambda, aws_s3, aws_iam
from constructs import Construct


class IndexerService(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        store_bucket: aws_s3.Bucket,
        index_id: str,
        index_config_bucket: str,
        index_config_key: str,
        memory_size: int,
        environment: dict[str, str],
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.lambda_function = aws_lambda.Function(
            self,
            id="Lambda",
            code=aws_lambda.Code.from_asset("../../quickwit/target/lambda/indexer"),
            runtime=aws_lambda.Runtime.PROVIDED_AL2,
            handler="N/A",
            environment={
                "QW_LAMBDA_INDEX_BUCKET": store_bucket.bucket_name,
                "QW_LAMBDA_METASTORE_BUCKET": store_bucket.bucket_name,
                "QW_LAMBDA_INDEX_ID": index_id,
                "QW_LAMBDA_INDEX_CONFIG_URI": f"s3://{index_config_bucket}/{index_config_key}",
                **environment,
            },
            timeout=aws_cdk.Duration.minutes(15),
            reserved_concurrent_executions=1,
            memory_size=memory_size,
            ephemeral_storage_size=aws_cdk.Size.gibibytes(10),
        )
        self.lambda_function.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=["s3:GetObject"],
                resources=[f"arn:aws:s3:::{index_config_bucket}/{index_config_key}"],
            )
        )
        store_bucket.grant_read_write(self.lambda_function)
