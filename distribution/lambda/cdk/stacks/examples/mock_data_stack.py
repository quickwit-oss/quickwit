import aws_cdk
from aws_cdk import (
    Stack,
    aws_apigateway,
    aws_lambda,
    aws_s3,
    aws_s3_assets,
    aws_s3_notifications,
    aws_events,
    aws_events_targets,
)
from constructs import Construct
import yaml

from ..services import quickwit_service

SEARCHER_FUNCTION_NAME_EXPORT_NAME = "mock-data-searcher-function-name"
INDEX_STORE_BUCKET_NAME_EXPORT_NAME = "mock-data-index-store-bucket-name"
SOURCE_BUCKET_NAME_EXPORT_NAME = "mock-data-source-bucket-name"
API_GATEWAY_EXPORT_NAME = "mock-data-api-gateway-url"


class Source(Construct):
    """An synthetic data source that generates mock data and pushes it to the
    indexer through a staging S3 bucket"""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        index_id: str,
        qw_svc: quickwit_service.QuickwitService,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)
        mock_data_bucket = aws_s3.Bucket(
            self,
            "mock-data",
            removal_policy=aws_cdk.RemovalPolicy.DESTROY,
            lifecycle_rules=[
                aws_s3.LifecycleRule(enabled=True, expiration=aws_cdk.Duration.days(1))
            ],
        )

        generator_lambda = aws_lambda.Function(
            self,
            id="MockDataGenerator",
            code=aws_lambda.Code.from_asset("resources/data-generator/"),
            runtime=aws_lambda.Runtime.PYTHON_3_10,
            handler="handler.lambda_handler",
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
            "ScheduledRule",
            schedule=aws_events.Schedule.rate(aws_cdk.Duration.minutes(5)),
        )
        rule.add_target(aws_events_targets.LambdaFunction(generator_lambda))

        mock_data_bucket.grant_read(qw_svc.indexer.lambda_function)
        mock_data_bucket.add_object_created_notification(
            aws_s3_notifications.LambdaDestination(qw_svc.indexer.lambda_function)
        )
        aws_cdk.CfnOutput(
            self,
            "source-bucket-name",
            value=mock_data_bucket.bucket_name,
            export_name=SOURCE_BUCKET_NAME_EXPORT_NAME,
        )


class SearchAPI(Construct):
    """An API Gateway example configuration to expose the Searcher Lambda
    function as a Quickwit search endpoint."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        index_id: str,
        qw_svc: quickwit_service.QuickwitService,
        api_key: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        api = aws_apigateway.RestApi(
            self,
            "quickwit-search-api",
            rest_api_name=f"Quickwit {index_id} search API",
            deploy=False,
        )
        searcher_integration = aws_apigateway.LambdaIntegration(
            qw_svc.searcher.lambda_function
        )
        search_resource = api.root.add_resource("v1").add_resource("{proxy+}")
        search_resource.add_method("POST", searcher_integration, api_key_required=True)
        search_resource.add_method("GET", searcher_integration, api_key_required=True)
        search_resource.add_method("PUT", searcher_integration, api_key_required=True)
        # Change the deployment id (api-deployment-x) each time the API changes,
        # otherwise changes are not deployed.
        api_deployment = aws_apigateway.Deployment(self, "api-deployment-1", api=api)
        api_stage = aws_apigateway.Stage(
            self, "api", deployment=api_deployment, stage_name="api"
        )
        plan = aws_apigateway.UsagePlan(
            self,
            "default-usage-plan",
            api_stages=[aws_apigateway.UsagePlanPerApiStage(api=api, stage=api_stage)],
            description="Usage plan for the Quickwit search API",
        )
        key = aws_apigateway.ApiKey(
            self,
            "default-api-key",
            value=api_key,
            description="Default API key for the Quickwit search API",
        )
        plan.add_api_key(key)
        api.deployment_stage = api_stage

        aws_cdk.CfnOutput(
            self,
            "search-api-url",
            value=api.url.rstrip("/") + search_resource.path,
            export_name=API_GATEWAY_EXPORT_NAME,
        )


class MockDataStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        indexer_package_location: str,
        searcher_package_location: str,
        search_api_key: str | None = None,
        **kwargs,
    ) -> None:
        """If `search_api_key` is not set, the search API is not deployed."""
        super().__init__(scope, construct_id, **kwargs)

        index_config_local_path = "resources/mock-sales.yaml"
        with open(index_config_local_path) as f:
            index_config_dict = yaml.safe_load(f)
            index_id = index_config_dict["index_id"]

        index_config = aws_s3_assets.Asset(
            self,
            "mock-data-index-config",
            path=index_config_local_path,
        )
        lambda_env = quickwit_service.extract_local_env()
        qw_svc = quickwit_service.QuickwitService(
            self,
            "Quickwit",
            index_id=index_id,
            index_config_bucket=index_config.s3_bucket_name,
            index_config_key=index_config.s3_object_key,
            indexer_environment=lambda_env,
            searcher_environment=lambda_env,
            indexer_package_location=indexer_package_location,
            searcher_package_location=searcher_package_location,
        )

        Source(self, "Source", index_id=index_id, qw_svc=qw_svc)

        if search_api_key is not None:
            SearchAPI(
                self,
                "SearchAPI",
                index_id=index_id,
                qw_svc=qw_svc,
                api_key=search_api_key,
            )

        aws_cdk.CfnOutput(
            self,
            "index-store-bucket-name",
            value=qw_svc.bucket.bucket_name,
            export_name=INDEX_STORE_BUCKET_NAME_EXPORT_NAME,
        )
        aws_cdk.CfnOutput(
            self,
            "searcher-function-name",
            value=qw_svc.searcher.lambda_function.function_name,
            export_name=SEARCHER_FUNCTION_NAME_EXPORT_NAME,
        )
