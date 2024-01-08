import aws_cdk
from aws_cdk import aws_lambda, aws_s3, PhysicalName
from constructs import Construct
from . import indexer_service, searcher_service


class QuickwitService(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        index_config_bucket: str,
        index_config_key: str,
        index_id: str,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.bucket = aws_s3.Bucket(
            self,
            "IndexStore",
            removal_policy=aws_cdk.RemovalPolicy.DESTROY,
        )
        self.indexer = indexer_service.IndexerService(
            self,
            "Indexer",
            store_bucket=self.bucket,
            index_id=index_id,
            index_config_bucket=index_config_bucket,
            index_config_key=index_config_key,
        )
        self.searcher = searcher_service.SearcherService(
            self,
            "Searcher",
            store_bucket=self.bucket,
            index_id=index_id,
        )
