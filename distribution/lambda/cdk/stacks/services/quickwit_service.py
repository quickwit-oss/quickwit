import os
from enum import Enum
from typing import Literal

import aws_cdk
from aws_cdk import aws_s3
from constructs import Construct

from . import indexer_service, searcher_service


def extract_local_env() -> dict[str, str]:
    """Extracts local environment variables that start with QW_LAMBDA_"""
    return {k: os.environ[k] for k in os.environ.keys() if k.startswith("QW_LAMBDA_")}


def package_location_from_env(type: Literal["searcher"] | Literal["indexer"]) -> str:
    path_var = f"{type.upper()}_PACKAGE_PATH"
    if path_var in os.environ:
        return os.environ[path_var]
    else:
        print(f"Could not infer the {type} package location. You must either:")
        print(f"- configure location in the QuickwitService construct")
        print(f"- configure the path using {path_var}")
        exit(1)


class QuickwitService(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        index_config_bucket: str,
        index_config_key: str,
        index_id: str,
        indexer_memory_size: int = 1024,
        indexer_environment: dict[str, str] = {},
        searcher_memory_size: int = 1024,
        searcher_environment: dict[str, str] = {},
        searcher_package_location: str | None = None,
        indexer_package_location: str | None = None,
        **kwargs,
    ) -> None:
        """Create a new Quickwit Lambda service construct node.

        If `{indexer|searcher}_package_location` is `None`, its value is inferred
        from the environment variable `{INDEXER|SEARCHER}_PACKAGE_PATH`.
        """
        super().__init__(scope, construct_id, **kwargs)
        if searcher_package_location is None:
            searcher_package_location = package_location_from_env("searcher")
        if indexer_package_location is None:
            indexer_package_location = package_location_from_env("indexer")
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
