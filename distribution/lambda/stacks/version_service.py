from constructs import Construct
from aws_cdk import aws_lambda


class VersionService(Construct):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        handler = aws_lambda.DockerImageFunction(
            self,
            id="HelloQuickwitVersion",
            function_name="HelloQuickwitVersion",
            code=aws_lambda.DockerImageCode.from_image_asset(
                directory="../../",
                target="quickwit-lambda",
                build_args={"CARGO_FEATURES": "lambda"},
            ),
        )
