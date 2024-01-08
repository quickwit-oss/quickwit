#!/usr/bin/env python3
import os

import aws_cdk as cdk

from cdk.stacks.base_stack import BaseQuickwitStack
from cdk.stacks.hdfs_stack import HdfsStack
from cdk.stacks.mock_data_stack import MockDataStack


BASE_STACK_NAME = "BaseQuickwitStack"
HDFS_STACK_NAME = "HdfsStack"
MOCK_DATA_STACK_NAME = "MockDataStack"


app = cdk.App()

BaseQuickwitStack(app, BASE_STACK_NAME)

HdfsStack(
    app,
    HDFS_STACK_NAME,
    env=cdk.Environment(
        account=os.getenv("CDK_ACCOUNT"), region=os.getenv("CDK_REGION")
    ),
)

MockDataStack(
    app,
    MOCK_DATA_STACK_NAME,
    env=cdk.Environment(
        account=os.getenv("CDK_ACCOUNT"), region=os.getenv("CDK_REGION")
    ),
)

app.synth()
