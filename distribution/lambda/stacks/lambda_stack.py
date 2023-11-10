from aws_cdk import Stack, aws_s3, CfnOutput, CfnParameter
from constructs import Construct

from . import indexer_service, searcher_service


class LambdaStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        index_id_param = CfnParameter(
            self,
            "quickwitIndexId",
            type="String",
            description="The ID of the Quickwit index",
        )

        bucket = aws_s3.Bucket(self, "index-store")

        index_config_uri = f"s3://{bucket.bucket_name}/index-conf/{index_id_param.value_as_string}.yaml"

        indexer_service.IndexerService(
            self,
            "IndexerService",
            store_bucket=bucket,
            index_id=index_id_param.value_as_string,
            index_config_uri=index_config_uri,
        )
        searcher_service.SearcherService(
            self,
            "SearcherService",
            store_bucket=bucket,
            index_id=index_id_param.value_as_string,
        )

        CfnOutput(
            self,
            "index-store-bucket-name",
            value=bucket.bucket_name,
            export_name="index-store-bucket-name",
        )

        CfnOutput(
            self,
            "index-config-uri",
            value=index_config_uri,
            export_name="index-config-uri",
        )
