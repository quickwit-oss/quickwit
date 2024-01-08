"""Helper scripts to test and explore the deployed infrastructure.

These functions are wrapped by the Makefile for convenience."""

import base64
import http.client
import os
import time

import boto3
from cdk.stacks import hdfs_stack, mock_data_stack
from cdk import app


region = os.environ["CDK_REGION"]

example_bucket = "quickwit-datasets-public.s3.amazonaws.com"
example_file = "hdfs-logs-multitenants-10000.json"

session = boto3.Session(region_name=region)


def _get_cloudformation_output_value(stack_name: str, export_name: str) -> str:
    client = session.client("cloudformation")
    stacks = client.describe_stacks(StackName=stack_name)["Stacks"]
    if len(stacks) != 1:
        print(f"Stack {stack_name} not identified uniquely, found {stacks}")
    outputs = stacks[0]["Outputs"]
    for output in outputs:
        if output["ExportName"] == export_name:
            return output["OutputValue"]
    else:
        print(f"Export name {export_name} not found in stack {stack_name}")
        exit(1)


def _format_lambda_output(lambda_resp, duration=None):
    if "FunctionError" in lambda_resp and lambda_resp["FunctionError"] != "":
        print("\n## FUNCTION ERROR:")
        print(lambda_resp["FunctionError"])
    print("\n## LOG TAIL:")
    print(base64.b64decode(lambda_resp["LogResult"]).decode())
    print("\n## RESPONSE:")
    print(lambda_resp["Payload"].read().decode())
    if duration is not None:
        print("\n## TOTAL INVOCATION DURATION:")
        print(duration)


def upload_hdfs_src_file():
    bucket_name = _get_cloudformation_output_value(
        app.HDFS_STACK_NAME, hdfs_stack.INDEX_STORE_BUCKET_NAME_EXPORT_NAME
    )
    target_uri = f"s3://{bucket_name}/{example_file}"
    print(f"upload src file to {target_uri}")
    conn = http.client.HTTPSConnection(example_bucket)
    conn.request("GET", f"/{example_file}")
    response = conn.getresponse()
    if response.status != 200:
        print(f"Failed to fetch dataset")
        exit(1)
    file_data = response.read()
    session.client("s3").put_object(
        Bucket=bucket_name, Body=file_data, Key=example_file
    )


def invoke_hdfs_indexer():
    function_name = _get_cloudformation_output_value(
        app.HDFS_STACK_NAME, hdfs_stack.INDEXER_FUNCTION_NAME_EXPORT_NAME
    )
    print(f"indexer function name: {function_name}")
    bucket_name = _get_cloudformation_output_value(
        app.HDFS_STACK_NAME, hdfs_stack.INDEX_STORE_BUCKET_NAME_EXPORT_NAME
    )
    source_uri = f"s3://{bucket_name}/{example_file}"
    print(f"src_file: {source_uri}")
    invoke_start = time.time()
    resp = session.client("lambda").invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=f"""{{ "source_uri": "{source_uri}" }}""",
    )
    _format_lambda_output(resp, time.time() - invoke_start)


def _invoke_searcher(stack_name: str, function_export_name: str, payload: str):
    function_name = _get_cloudformation_output_value(stack_name, function_export_name)
    print(f"searcher function name: {function_name}")
    invoke_start = time.time()
    resp = session.client("lambda").invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=payload,
    )
    _format_lambda_output(resp, time.time() - invoke_start)


def invoke_hdfs_searcher():
    _invoke_searcher(
        app.HDFS_STACK_NAME,
        hdfs_stack.SEARCHER_FUNCTION_NAME_EXPORT_NAME,
        """{"query": "tenant_id:1 AND HDFS_WRITE", "sort_by": "timestamp", "max_hits": 10}""",
    )


def invoke_mock_data_searcher():
    _invoke_searcher(
        app.MOCK_DATA_STACK_NAME,
        mock_data_stack.SEARCHER_FUNCTION_NAME_EXPORT_NAME,
        """{"query": "id:1", "sort_by": "ts", "max_hits": 10}""",
    )
