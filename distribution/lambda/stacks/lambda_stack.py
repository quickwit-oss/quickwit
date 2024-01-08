from aws_cdk import Stack, aws_s3, CfnOutput
from constructs import Construct

from . import indexing_service, query_service


class LambdaStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket = aws_s3.Bucket(self, "index-store")
        indexing_service.IndexingService(self, "IndexingService", store_bucket=bucket)
        query_service.QueryService(self, "QueryService", store_bucket=bucket)

        CfnOutput(
            self,
            "index-store-bucket-name",
            value=bucket.bucket_name,
            export_name="index-store-bucket-name",
        )
