"""Helper scripts to test and explore the deployed infrastructure.

These functions are wrapped by the Makefile for convenience."""

import base64
import http.client
import os
import time
from urllib.parse import urlparse

import boto3

region = os.environ["CDK_REGION"]
index_id = os.environ["INDEX_ID"]

stack_name = "LambdaStack"
example_bucket = "quickwit-datasets-public.s3.amazonaws.com"
example_file = "hdfs-logs-multitenants-10000.json"
index_config_path = "../../config/tutorials/hdfs-logs/index-config.yaml"

session = boto3.Session(region_name=region)


def _get_cloudformation_output_value(export_name: str) -> str:
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


def upload_index_config():
    target_uri = _get_cloudformation_output_value("index-config-uri")
    print(f"upload src file to {target_uri}")
    target_uri_parsed = urlparse(target_uri, allow_fragments=False)
    with open(index_config_path, "rb") as f:
        session.client("s3").put_object(
            Bucket=target_uri_parsed.netloc, Body=f, Key=target_uri_parsed.path[1:]
        )


def upload_src_file():
    bucket_name = _get_cloudformation_output_value("index-store-bucket-name")
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


def invoke_indexer():
    function_name = _get_cloudformation_output_value("indexer-function-name")
    print(f"indexer function name: {function_name}")
    bucket_name = _get_cloudformation_output_value("index-store-bucket-name")
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


def invoke_searcher():
    function_name = _get_cloudformation_output_value("searcher-function-name")
    print(f"searcher function name: {function_name}")
    invoke_start = time.time()
    resp = session.client("lambda").invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload="""{"query": "tenant_id:1 AND HDFS_WRITE", "sort_by": "timestamp", "max_hits": 10}""",
    )
    _format_lambda_output(resp, time.time() - invoke_start)
