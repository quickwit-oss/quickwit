"""Helper scripts to test and explore the deployed infrastructure.

These functions are wrapped by the Makefile for convenience."""

import os
import boto3
import base64
import http.client

region = os.environ["CDK_REGION"]
stack_name = "LambdaStack"
example_bucket = "quickwit-datasets-public.s3.amazonaws.com"
example_file = "hdfs-logs-multitenants-10000.json"

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


def _format_lambda_output(lambda_resp):
    if "FunctionError" in lambda_resp and lambda_resp["FunctionError"] != "":
        print("\n## FUNCTION ERROR:")
        print(lambda_resp["FunctionError"])
    print("\n## LOG TAIL:")
    print(base64.b64decode(lambda_resp["LogResult"]).decode())
    print("\n## RESPONSE:")
    print(lambda_resp["Payload"].read().decode())


def upload_src_file():
    bucket_name = _get_cloudformation_output_value("index-store-bucket-name")
    source_uri = f"s3://{bucket_name}/{example_file}"
    print(f"upload src file to {source_uri}")
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
    resp = session.client("lambda").invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=f"""{{ "source_uri": "{source_uri}" }}""",
    )
    _format_lambda_output(resp)


def invoke_searcher():
    function_name = _get_cloudformation_output_value("searcher-function-name")
    print(f"searcher function name: {function_name}")
    resp = session.client("lambda").invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload="{}",
    )
    _format_lambda_output(resp)
