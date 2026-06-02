workspace(name = "com_datadoghq_pomsky")

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("//:rules/cnab/version.bzl", "CNAB_TOOLS_VERSION")

git_repository(
    name = "rules_oci_bootstrap",
    remote = "https://github.com/DataDog/rules_oci_bootstrap.git",
    commit = "bd0ca9ffe7c7706b0979a131cc48a7d24f4bfbbd",
)


load("@rules_oci_bootstrap//:defs.bzl", "oci_blob_pull")

oci_blob_pull(
    name = "com_datadoghq_cnab_tools",
    registry = "registry.ddbuild.io",
    repository = "cnab-tools/rules_cnab",
    digest = CNAB_TOOLS_VERSION,
    extract = True,
    type = "tar.gz",
)

load("@com_datadoghq_cnab_tools//rules:deps.bzl", "cnab_tools_dependencies")
cnab_tools_dependencies()

load("@com_datadoghq_cnab_tools//rules/setup:cnab_tools.bzl", "cnab_tools_setup")
cnab_tools_setup()

load("@com_datadoghq_cnab_tools//rules/setup:rules_python.bzl", "rules_python_setup")
rules_python_setup()

load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains")

GO_VERSION = "1.24.5"


load("@com_datadoghq_cnab_tools//rules/setup:rules_go.bzl", "rules_go_setup")
# Don't let it fetch its own SDK; we'll provide one next.
rules_go_setup(go_version = GO_VERSION)


git_repository(
    name = "com_datadoghq_datacenter_config",
    commit = "cdc36e161294148f4cd1ddc1c4f7063acdbf1855",
    remote = "https://github.com/DataDog/datacenter-config.git",
)

load("@com_datadoghq_datacenter_config//rules:deps.bzl", "datacenter_config_dependencies")
datacenter_config_dependencies()

load("@com_datadoghq_datacenter_config//rules/setup:rules_docker.bzl", "rules_docker_setup")
rules_docker_setup()

git_repository(
    name = "pomsky_helm_charts",
    remote = "https://github.com/DataDog/pomsky-helm-charts.git",
    commit = "eebc6882a6cbfde577ebf12f983ada249227fb4f",
)
