import os

import aws_cdk
from aws_cdk import aws_s3
from constructs import Construct

from . import indexer_service, searcher_service

# Using 3008MB as default because higher memory configurations need to be
# enabled for each AWS account through the support.
DEFAULT_LAMBDA_MEMORY_SIZE = 3008


def extract_local_env() -> dict[str, str]:
    """Extracts local environment variables that start with QW_LAMBDA_"""
    return {k: os.environ[k] for k in os.environ.keys() if k.startswith("QW_LAMBDA_")}


class QuickwitService(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        index_config_bucket: str,
        index_config_key: str,
        index_id: str,
        searcher_package_location: str,
        indexer_package_location: str,
        indexer_memory_size: int = DEFAULT_LAMBDA_MEMORY_SIZE,
        indexer_environment: dict[str, str] = {},
        searcher_memory_size: int = DEFAULT_LAMBDA_MEMORY_SIZE,
        searcher_environment: dict[str, str] = {},
        **kwargs,
    ) -> None:
        """Create a new Quickwit Lambda service construct node.

        `{indexer|searcher}_package_location` is the path of the `zip` asset for
        the Lambda function.
        """
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
            memory_size=indexer_memory_size,
            environment=indexer_environment,
            asset_path=indexer_package_location,
        )
        self.searcher = searcher_service.SearcherService(
            self,
            "Searcher",
            store_bucket=self.bucket,
            index_id=index_id,
            memory_size=searcher_memory_size,
            environment=searcher_environment,
            asset_path=searcher_package_location,
        )
