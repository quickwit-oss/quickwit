from constructs import Construct
from aws_cdk import aws_lambda, aws_s3, Duration, CfnOutput


class SearcherService(Construct):
    def __init__(
        self, scope: Construct, construct_id: str, store_bucket: aws_s3.Bucket, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        handler = aws_lambda.Function(
            self,
            id="quickwit-searcher",
            code=aws_lambda.Code.from_asset("../../quickwit/target/lambda/searcher"),
            runtime=aws_lambda.Runtime.PROVIDED_AL2,
            handler="N/A",
            environment={
                "INDEX_BUCKET": store_bucket.bucket_name,
                "METASTORE_BUCKET": store_bucket.bucket_name,
            },
            timeout=Duration.seconds(30),
        )

        store_bucket.grant_read_write(handler)

        CfnOutput(
            self,
            "searcher-function-name",
            value=handler.function_name,
            export_name="searcher-function-name",
        )
