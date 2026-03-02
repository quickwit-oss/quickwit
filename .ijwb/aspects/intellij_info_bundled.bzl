"""Bazel-specific intellij aspect."""

load(
    ":intellij_info_impl_bundled.bzl",
    "intellij_info_aspect_impl",
    "is_valid_aspect_target",
    "make_intellij_info_aspect",
)

EXTRA_DEPS = [
    "embed",  # From go rules (bazel only)
    "_cc_toolchain",  # From rules_cc (bazel only)
    "_kt_toolchain",  # From rules_kotlin (bazel only)
]

TOOLCHAIN_TYPE_DEPS = [
    "@@bazel_tools//tools/cpp:toolchain_type",  # For rules_cc
]

def tool_label(tool_name):
    """Returns a label that points to a tool target in the bundled aspect workspace."""
    return Label("tools/" + tool_name)

def get_go_import_path(ctx):
    """Returns the import path for a go target."""
    import_path = getattr(ctx.rule.attr, "importpath", None)
    if import_path:
        return import_path
    prefix = None
    if hasattr(ctx.rule.attr, "_go_prefix"):
        prefix = ctx.rule.attr._go_prefix.go_prefix
    if not prefix:
        return None
    import_path = prefix
    if ctx.label.package:
        import_path += "/" + ctx.label.package
    if ctx.label.name != "go_default_library":
        import_path += "/" + ctx.label.name
    return import_path

def is_go_proto_library(target, _ctx):
    return hasattr(target[OutputGroupInfo], "go_generated_srcs")

def get_go_proto_library_generated_srcs(target):
    files = target[OutputGroupInfo].go_generated_srcs.to_list()
    return [f for f in files if f.basename.endswith(".go")]

def get_py_launcher(target, ctx):
    """Returns the python launcher for a given rule."""

    # Used by other implementations of get_launcher
    _ = target  # @unused
    attr = ctx.rule.attr
    if hasattr(attr, "_launcher") and attr._launcher != None:
        return str(attr._launcher.label)
    return None

def _collect_targets_from_toolchains(ctx, toolchain_types):
    """Returns a list of targets for the given toolchain types."""
    result = []

    for toolchain_type in toolchain_types:
        # toolchains attribute only available in Bazel 8+
        toolchains = getattr(ctx.rule, "toolchains", [])

        if toolchain_type in toolchains:
            if is_valid_aspect_target(toolchains[toolchain_type]):
                result.append(toolchains[toolchain_type])

    return result

semantics = struct(
    tool_label = tool_label,
    toolchains_propagation = struct(
        toolchain_types = TOOLCHAIN_TYPE_DEPS,
        collect_toolchain_deps = _collect_targets_from_toolchains,
    ),
    extra_deps = EXTRA_DEPS,
    extra_required_aspect_providers = [],
    go = struct(
        get_import_path = get_go_import_path,
        is_proto_library = is_go_proto_library,
        get_proto_library_generated_srcs = get_go_proto_library_generated_srcs,
    ),
    py = struct(
        get_launcher = get_py_launcher,
    ),
)

def _aspect_impl(target, ctx):
    return intellij_info_aspect_impl(target, ctx, semantics)

# TEMPLATE-INCLUDE-BEGIN
intellij_info_aspect = make_intellij_info_aspect(
    _aspect_impl,
    semantics,
)
# TEMPLATE-INCLUDE-END

