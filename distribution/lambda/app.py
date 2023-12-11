#!/usr/bin/env python3
import os

import aws_cdk as cdk

from stacks.lambda_stack import LambdaStack


app = cdk.App()
LambdaStack(
    app,
    "LambdaStack",
    # If you don't specify 'env', this stack will be environment-agnostic.
    # Account/Region-dependent features and context lookups will not work,
    # but a single synthesized template can be deployed anywhere.
    env=cdk.Environment(
        account=os.getenv("CDK_ACCOUNT"), region=os.getenv("CDK_REGION")
    ),
)


app.synth()
