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

load("@io_bazel_rules_go//go:deps.bzl", "go_download_sdk", "go_register_toolchains")

GO_VERSION = "1.24.5"

GO_LINUX_AMD64_DIGEST = "10ad9e86233e74c0f6590fe5426895de6bf388964210eac34a6d83f38918ecdc"
GO_LINUX_ARM64_DIGEST = "0df02e6aeb3d3c06c95ff201d575907c736d6c62cfa4b6934c11203f1d600ffa"
GO_DARWIN_AMD64_DIGEST = "2fe5f3866b8fbcd20625d531f81019e574376b8a840b0a096d8a2180308b1672"
GO_DARWIN_ARM64_DIGEST = "92d30a678f306c327c544758f2d2fa5515aa60abe9dba4ca35fbf9b8bfc53212"

BINARIES_GO_URL = "https://depot-read-api-generic.us1.ddbuild.io/magicmirror/magicmirror/@current/runtime/go/go{}/{}".format(
    GO_VERSION, "{}"
)

GO_SDKS = {
    "darwin_amd64": ("go{}.darwin-amd64.tar.gz".format(GO_VERSION), GO_DARWIN_AMD64_DIGEST),
    "darwin_arm64": ("go{}.darwin-arm64.tar.gz".format(GO_VERSION), GO_DARWIN_ARM64_DIGEST),
    "linux_amd64": ("go{}.linux-amd64.tar.gz".format(GO_VERSION), GO_LINUX_AMD64_DIGEST),
    "linux_arm64": ("go{}.linux-arm64.tar.gz".format(GO_VERSION), GO_LINUX_ARM64_DIGEST),
}

go_download_sdk(
    name = "go_sdk",
    sdks = GO_SDKS,
    urls = [BINARIES_GO_URL],
    version = GO_VERSION,
)

load("@com_datadoghq_cnab_tools//rules/setup:rules_go.bzl", "rules_go_setup")
# Don't let it fetch its own SDK; we'll provide one next.
rules_go_setup(go_version = None)


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
    commit = "ec97a626d3d935a218ef295892bf4487365668f3",
)
