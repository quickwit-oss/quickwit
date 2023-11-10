from aws_cdk import Stack
from constructs import Construct
from . import version_service


class LambdaStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        version_service.VersionService(self, "VersionService")
