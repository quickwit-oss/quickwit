#!/usr/bin/env python3
import os
from typing import Literal

import aws_cdk as cdk

from cdk.stacks.services.quickwit_service import DEFAULT_LAMBDA_MEMORY_SIZE
from cdk.stacks.examples.hdfs_stack import HdfsStack
from cdk.stacks.examples.mock_data_stack import MockDataStack

HDFS_STACK_NAME = "HdfsStack"
MOCK_DATA_STACK_NAME = "MockDataStack"


def package_location_from_env(type: Literal["searcher"] | Literal["indexer"]) -> str:
    path_var = f"{type.upper()}_PACKAGE_PATH"
    if path_var in os.environ:
        return os.environ[path_var]
    else:
        print(
            f"Could not infer the {type} package location. Configure it using the {path_var} environment variable"
        )
        exit(1)


app = cdk.App()

HdfsStack(
    app,
    HDFS_STACK_NAME,
    env=cdk.Environment(
        account=os.getenv("CDK_ACCOUNT"), region=os.getenv("CDK_REGION")
    ),
    indexer_memory_size=int(
        os.environ.get("INDEXER_MEMORY_SIZE", DEFAULT_LAMBDA_MEMORY_SIZE)
    ),
    searcher_memory_size=int(
        os.environ.get("SEARCHER_MEMORY_SIZE", DEFAULT_LAMBDA_MEMORY_SIZE)
    ),
    indexer_package_location=package_location_from_env("indexer"),
    searcher_package_location=package_location_from_env("searcher"),
)

MockDataStack(
    app,
    MOCK_DATA_STACK_NAME,
    env=cdk.Environment(
        account=os.getenv("CDK_ACCOUNT"), region=os.getenv("CDK_REGION")
    ),
    indexer_package_location=package_location_from_env("indexer"),
    searcher_package_location=package_location_from_env("searcher"),
    search_api_key=os.getenv("SEARCHER_API_KEY", None),
)

app.synth()
