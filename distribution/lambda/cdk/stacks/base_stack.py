from typing import Optional

from aws_cdk import Stack, CfnParameter
from constructs import Construct

from .services import quickwit_service


class BaseQuickwitStack(Stack):
    """Base componentes required to run Quickwit serverless."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        index_id_param = CfnParameter(
            self,
            "quickwitIndexId",
            type="String",
            description="The ID of the Quickwit index",
        )
        index_config_bucket_param = CfnParameter(
            self,
            "quickwitIndexConfigBucket",
            type="String",
            description="The S3 bucket name of the Quickwit index config",
        )
        index_config_key_param = CfnParameter(
            self,
            "quickwitIndexConfigKey",
            type="String",
            description="The S3 object key of the Quickwit index config",
        )

        quickwit_service.QuickwitService(
            self,
            "Quickwit",
            index_id=index_id_param.value_as_string,
            index_config_bucket=index_config_bucket_param.value_as_string,
            index_config_key=index_config_key_param.value_as_string,
        )
