from constructs import Construct
from aws_cdk import aws_lambda


class VersionService(Construct):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        aws_lambda.Function(
            self,
            id="HelloQuickwitVersion",
            function_name="HelloQuickwitVersion",
            code=aws_lambda.Code.from_asset(
                "../../quickwit/target/lambda/quickwit-lambda"
            ),
            runtime=aws_lambda.Runtime.PROVIDED_AL2,
            handler="N/A",
        )
