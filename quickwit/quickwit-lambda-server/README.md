# Lambda search

Quickwit makes it possible to run leaf search on lambdas.

In order to make it possible to update quickwit AND the function you
can have Quickwit itself can deploy/create the function.

For that purpose Quickwit client's deployer embeds the lambda function zip file
within Quickwit's binary.

That binary needs to be built and released by manually triggerring the `publish_lambda.yaml` github action.
By default this action only creates a draft release. You will need to manually publish the release for the url to be publicly downloadable.
The URL in quickwit-lambda-client/build.rs then needs to be manually updated.
