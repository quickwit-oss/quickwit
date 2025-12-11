# cloudprem-ui

CloudPrem UI is a local log explorer for CloudPrem. It is a [static-app](https://datadoghq.atlassian.net/wiki/x/SoRUrQ) created in [web-ui](https://github.com/DataDog/web-ui/tree/preprod/static-apps/cloudprem-ui). CI in `web-ui` builds the app and pushes a compressed artifact to `binaries-ddbuild-io-prod/cloudprem-ui`. This allows us to use existing Datadog components, [DRUIDS](https://druids.us1.prod.dog/), and [Dataviz](https://dataviz.us1.prod.dog/) which makes the app feel similar to SaaS. Pomsky pulls it from `ddbuild` and embeds it into its binary. This is done in `/quickwit-serve/src/cloudprem_ui_handler.rs`.

## Available Scripts


In the project directory, you can run:


### `make load-cloudprem-ui`

Loads CloudPrem UI from `https://binaries.ddbuild.io/cloudprem-ui/$(CLOUDPREM_UI_ENV)/v/$(CLOUDPREM_UI_VERSION)/dist.tar.gz`. You can set `CLOUDPREM_UI_ENV` and `CLOUDPREM_UI_VERSION` environment variables to select the specifc build. After running this, you can run pomsky as normal and visit `localhost:7280` to access the CloudPrem UI.

## Environment and Version

`$(CLOUDPREM_UI_ENV)` can be `prod`, `staging`, or `hash`. For most cases, you will want to pull the image from `prod`. If you are working on a specifc branch is `web-ui` and want to test the changes, you can pull the image from `hash` or push that branch to staging and pull the image from `staging`. 

Currently, the version number is bumped manually. On the `web-ui` side, the convention will be:

- `0.1.0`: This number will be updated when we want to release a new version to customers.
- `0.1.0-dev`: We will use `-dev` as the suffix for builds we don't want to release to customers. This is because `web-ui` publishes a new artifact everytime changes are made to prod, which is not ideal for how often we release CloudPrem.
