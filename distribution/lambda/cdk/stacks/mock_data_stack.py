import aws_cdk
from aws_cdk import (
    Stack,
    aws_lambda,
    aws_s3,
    aws_s3_assets,
    aws_s3_notifications,
    aws_events,
    aws_events_targets,
)
from constructs import Construct
import yaml

from .services import quickwit_service

SEARCHER_FUNCTION_NAME_EXPORT_NAME = "mock-data-searcher-function-name"


class MockDataStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        index_config_local_path = "resources/mock-sales.yaml"
        with open(index_config_local_path) as f:
            index_config_dict = yaml.safe_load(f)
            index_id = index_config_dict["index_id"]

        mock_data_bucket = aws_s3.Bucket(
            self,
            "mock-data",
            removal_policy=aws_cdk.RemovalPolicy.DESTROY,
        )

        with open("resources/data-generator.py") as f:
            lambda_code = f.read()
        generator_lambda = aws_lambda.Function(
            self,
            id="MockDataGenerator",
            code=aws_lambda.Code.from_inline(lambda_code),
            runtime=aws_lambda.Runtime.PYTHON_3_10,
            handler="index.lambda_handler",
            environment={
                "BUCKET_NAME": mock_data_bucket.bucket_name,
                "PREFIX": index_id,
            },
            timeout=aws_cdk.Duration.seconds(30),
            memory_size=1024,
        )
        mock_data_bucket.grant_read_write(generator_lambda)
        rule = aws_events.Rule(
            self,
            "Rule",
            schedule=aws_events.Schedule.rate(aws_cdk.Duration.minutes(5)),
        )
        rule.add_target(aws_events_targets.LambdaFunction(generator_lambda))

        index_config = aws_s3_assets.Asset(
            self,
            "mock-data-index-config",
            path=index_config_local_path,
        )
        qw_svc = quickwit_service.QuickwitService(
            self,
            "Quickwit",
            index_id=index_id,
            index_config_bucket=index_config.s3_bucket_name,
            index_config_key=index_config.s3_object_key,
        )
        mock_data_bucket.grant_read(qw_svc.indexer.lambda_function)
        mock_data_bucket.add_object_created_notification(
            aws_s3_notifications.LambdaDestination(qw_svc.indexer.lambda_function)
        )

        aws_cdk.CfnOutput(
            self,
            "searcher-function-name",
            value=qw_svc.searcher.lambda_function.function_name,
            export_name=SEARCHER_FUNCTION_NAME_EXPORT_NAME,
        )
