workspace(name = "com_datadoghq_pomsky")

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

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
    digest = "sha256:2fb2cf5e3bd3d655a8996918b626fd9234b2d28e996504e675076a2e75ae276f",
    extract = True,
    type = "tar.gz",
)

load("@com_datadoghq_cnab_tools//rules:deps.bzl", "cnab_tools_dependencies")
cnab_tools_dependencies()

load("@com_datadoghq_cnab_tools//rules/setup:cnab_tools.bzl", "cnab_tools_setup")
cnab_tools_setup()

load("@com_datadoghq_cnab_tools//rules/setup:rules_go.bzl", "rules_go_setup")
rules_go_setup()


git_repository(
    name = "com_datadoghq_datacenter_config",
    commit = "96a0b02f886a4e1ec82be1f9d0bd82e4b0d0508f",
    remote = "https://github.com/DataDog/datacenter-config.git",
)

load("@com_datadoghq_datacenter_config//rules:deps.bzl", "datacenter_config_dependencies")
datacenter_config_dependencies()

load("@com_datadoghq_datacenter_config//rules/setup:rules_docker.bzl", "rules_docker_setup")
rules_docker_setup()

git_repository(
    name = "pomsky_helm_charts",
    remote = "https://github.com/DataDog/pomsky-helm-charts.git",
    commit = "74655fc26d99f8b115ff65396e4a383e2578b3cf",
)

