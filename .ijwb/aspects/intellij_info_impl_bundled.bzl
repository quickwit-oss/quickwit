"""Implementation of IntelliJ-specific information collecting aspect."""

load(
    "@bazel_tools//tools/build_defs/cc:action_names.bzl",
    "ACTION_NAMES",
)
load(
    ":artifacts.bzl",
    "artifact_location",
    "artifacts_from_target_list_attr",
    "is_external_artifact",
    "sources_from_target",
    "struct_omit_none",
    "to_artifact_location",
)
load(":cc_info.bzl", "CC_USE_GET_TOOL_FOR_ACTION")
load(":code_generator_info.bzl", "CODE_GENERATOR_RULE_NAMES")
load(":java_info.bzl", "get_java_info", "get_provider_from_target", "java_info_in_target", "java_info_reference")
load(
    ":make_variables.bzl",
    "expand_make_variables",
)
load(":python_info.bzl", "get_py_info", "py_info_in_target")

IntelliJInfo = provider(
    doc = "Collected information about the targets visited by the aspect.",
    fields = [
        "export_deps",
        "kind",
        "output_groups",
        "target_key",
    ],
)

# Defensive list of features that can appear in the C++ toolchain, but which we
# definitely don't want to enable (when enabled, they'd contribute command line
# flags that don't make sense in the context of intellij info).
UNSUPPORTED_FEATURES = [
    "thin_lto",
    "module_maps",
    "use_header_modules",
    "fdo_instrument",
    "fdo_optimize",
]

# Compile-time dependency attributes, grouped by type.
DEPS = [
    "_stl",  # From cc rules
    "malloc",  # From cc_binary rules
    "implementation_deps",  # From cc_library rules
    "_java_toolchain",  # From java rules
    "deps",
    "jars",  # from java_import rules
    "exports",
    "java_lib",  # From old proto_library rules
    "_android_sdk",  # from android rules
    "aidl_lib",  # from android_sdk
    "_scala_toolchain",  # From scala rules
    "test_app",  # android_instrumentation_test
    "instruments",  # android_instrumentation_test
    "tests",  # From test_suite
    "compilers",  # From go_proto_library
    "associates",  # From kotlin rules
]

# Run-time dependency attributes, grouped by type.
RUNTIME_DEPS = [
    "runtime_deps",
]

PREREQUISITE_DEPS = []

# Dependency type enum
COMPILE_TIME = 0

RUNTIME = 1

# PythonVersion enum; must match PyIdeInfo.PythonVersion
PY2 = 1

PY3 = 2

# PythonCompatVersion enum; must match PyIdeInfo.PythonSrcsVersion
SRC_PY2 = 1

SRC_PY3 = 2

SRC_PY2AND3 = 3

SRC_PY2ONLY = 4

SRC_PY3ONLY = 5

##### Helpers

def run_jar(ctx, jar, **kwargs):
    """Runs a jar using the current java runtime used to run this bazel instance.

    Finds the current java runtime and uses the java executable to run the provided jar. The jar
    file should be a self contained _deploy jar.
    """

    host_java = get_provider_from_target("JavaRuntimeInfo", ctx.attr._java_runtime)

    return ctx.actions.run_shell(
        tools = depset([jar], transitive = [host_java.files]),
        command = "%s -jar %s $@" % (host_java.java_executable_exec_path, jar.path),
        **kwargs
    )

def get_code_generator_rule_names(ctx, language_name):
    """Supplies a list of Rule names for code generation for the language specified

    For some languages, it is possible to specify Rules' names that are interpreted as
    code-generators for the language. These Rules' names are specified as attrs and are provided to
    the aspect using the `AspectStrategy#AspectParameter` in the plugin logic.
    """

    if not language_name:
        fail("the `language_name` must be provided")

    if hasattr(CODE_GENERATOR_RULE_NAMES, language_name):
        return getattr(CODE_GENERATOR_RULE_NAMES, language_name)

    return []

def source_directory_tuple(resource_file):
    """Creates a tuple of (exec_path, root_exec_path_fragment, is_source, is_external)."""
    relative_path = str(android_common.resource_source_directory(resource_file))
    root_exec_path_fragment = resource_file.root.path if not resource_file.is_source else None
    return (
        relative_path if resource_file.is_source else root_exec_path_fragment + "/" + relative_path,
        root_exec_path_fragment,
        resource_file.is_source,
        is_external_artifact(resource_file.owner),
    )

def get_res_artifacts(resources):
    """Get a map from the res folder to the set of resource files within that folder.

    Args:
      resources: all resources of a target

    Returns:
       a map from the res folder to the set of resource files within that folder (as a tuple of path segments)
    """
    res_artifacts = dict()
    for resource in resources:
        for file in resource.files.to_list():
            res_folder = source_directory_tuple(file)
            res_artifacts.setdefault(res_folder, []).append(file)
    return res_artifacts

def build_file_artifact_location(ctx):
    """Creates an ArtifactLocation proto representing a location of a given BUILD file."""
    return to_artifact_location(
        ctx.label.package + "/BUILD",
        ctx.label.package + "/BUILD",
        True,
        is_external_artifact(ctx.label),
    )

# https://github.com/bazelbuild/bazel/issues/18966
def _list_or_depset_to_list(list_or_depset):
    if hasattr(list_or_depset, "to_list"):
        return list_or_depset.to_list()
    return list_or_depset

def get_source_jars(output):
    if hasattr(output, "source_jars"):
        return _list_or_depset_to_list(output.source_jars)
    if hasattr(output, "source_jar"):
        return [output.source_jar]
    return []

def library_artifact(java_output, rule_kind = None):
    """Creates a LibraryArtifact representing a given java_output."""
    if java_output == None or java_output.class_jar == None:
        return None
    src_jars = get_source_jars(java_output)

    if rule_kind != None and rule_kind.startswith("scala"):
        interface_jar = None
    else:
        interface_jar = artifact_location(java_output.ijar)

    return struct_omit_none(
        interface_jar = interface_jar,
        jar = artifact_location(java_output.class_jar),
        source_jar = artifact_location(src_jars[0]) if src_jars else None,
        source_jars = [artifact_location(f) for f in src_jars],
    )

def annotation_processing_jars(generated_class_jar, generated_source_jar):
    """Creates a LibraryArtifact representing Java annotation processing jars."""
    src_jar = generated_source_jar
    return struct_omit_none(
        jar = artifact_location(generated_class_jar),
        source_jar = artifact_location(src_jar),
        source_jars = [artifact_location(src_jar)] if src_jar else None,
    )

def jars_from_output(output):
    """Collect jars for intellij-resolve-files from Java output."""
    if output == None:
        return []
    source_jars = get_source_jars(output)
    return [
        jar
        for jar in ([output.ijar if len(source_jars) > 0 and output.ijar else output.class_jar] + source_jars)
        if jar != None and not jar.is_source
    ]

def _collect_target_from_attr(rule_attrs, attr_name, result):
    """Collects the targets from the given attr into the result."""
    if not hasattr(rule_attrs, attr_name):
        return
    attr_value = getattr(rule_attrs, attr_name)
    type_name = type(attr_value)
    if type_name == "Target":
        result.append(attr_value)
    elif type_name == "list":
        result.extend(attr_value)

def collect_targets_from_attrs(rule_attrs, attrs):
    """Returns a list of targets from the given attributes."""
    result = []
    for attr_name in attrs:
        _collect_target_from_attr(rule_attrs, attr_name, result)
    return [target for target in result if is_valid_aspect_target(target)]

def targets_to_labels(targets):
    """Returns a set of label strings for the given targets."""
    return depset([str(target.label) for target in targets])

def list_omit_none(value):
    """Returns a list of the value, or the empty list if None."""
    return [value] if value else []

def is_valid_aspect_target(target):
    """Returns whether the target has had the aspect run on it."""
    return IntelliJInfo in target

def get_aspect_ids(ctx):
    """Returns the all aspect ids, filtering out self."""
    aspect_ids = None
    if hasattr(ctx, "aspect_ids"):
        aspect_ids = ctx.aspect_ids
    else:
        return None
    return [aspect_id for aspect_id in aspect_ids if "intellij_info_aspect" not in aspect_id]

def _is_language_specific_proto_library(ctx, target, semantics):
    """Returns True if the target is a proto library with attached language-specific aspect."""
    if ctx.rule.kind != "proto_library":
        return False
    if java_info_in_target(target):
        return True
    if CcInfo in target:
        return True
    if semantics.go.is_proto_library(target, ctx):
        return True
    return False

def stringify_label(label):
    """Stringifies a label, making sure any leading '@'s are stripped from main repo labels."""
    s = str(label)

    # If the label is in the main repo, make sure any leading '@'s are stripped so that tests are
    # okay with the fixture setups.
    return s.lstrip("@") if s.startswith("@@//") or s.startswith("@//") else s

def make_target_key(label, aspect_ids):
    """Returns a TargetKey proto struct from a target."""
    return struct_omit_none(
        aspect_ids = tuple(aspect_ids) if aspect_ids else None,
        label = stringify_label(label),
    )

def make_dep(dep, dependency_type):
    """Returns a Dependency proto struct."""
    return struct(
        dependency_type = dependency_type,
        target = dep[IntelliJInfo].target_key,
    )

def make_deps(deps, dependency_type):
    """Returns a list of Dependency proto structs."""
    return [make_dep(dep, dependency_type) for dep in deps]

def make_dep_from_label(label, dependency_type):
    """Returns a Dependency proto struct from a label."""
    return struct(
        dependency_type = dependency_type,
        target = struct(label = stringify_label(label)),
    )

def update_sync_output_groups(groups_dict, key, new_set):
    """Updates all sync-relevant output groups associated with 'key'.

    This is currently the [key] output group itself, together with [key]-outputs
    and [key]-direct-deps.

    Args:
      groups_dict: the output groups dict, from group name to artifact depset.
      key: the base output group name.
      new_set: a depset of artifacts to add to the output groups.
    """
    update_set_in_dict(groups_dict, key, new_set)
    update_set_in_dict(groups_dict, key + "-outputs", new_set)
    update_set_in_dict(groups_dict, key + "-direct-deps", new_set)

def update_set_in_dict(input_dict, key, other_set):
    """Updates depset in dict, merging it with another depset."""
    input_dict[key] = depset(transitive = [input_dict.get(key, depset()), other_set])

def _get_output_mnemonic(ctx):
    """Gives the output directory mnemonic for some target context."""
    return ctx.bin_dir.path.split("/")[1]

def _get_python_version(ctx):
    return PY3

_SRCS_VERSION_MAPPING = {
    "PY2": SRC_PY2,
    "PY3": SRC_PY3,
    "PY2AND3": SRC_PY2AND3,
    "PY2ONLY": SRC_PY2ONLY,
    "PY3ONLY": SRC_PY3ONLY,
}

def _get_python_srcs_version(ctx):
    srcs_version = getattr(ctx.rule.attr, "srcs_version", "PY2AND3")
    return _SRCS_VERSION_MAPPING.get(srcs_version, default = SRC_PY2AND3)

def _do_starlark_string_expansion(ctx, name, strings, extra_targets = [], tokenization = True):
    # first, expand all starlark predefined paths:
    #   location, locations, rootpath, rootpaths, execpath, execpaths
    strings = [ctx.expand_location(value, targets = extra_targets) for value in strings]

    # then expand any regular GNU make style variables
    return expand_make_variables(ctx, tokenization, strings)

##### Builders for individual parts of the aspect output

def collect_py_info(target, ctx, semantics, ide_info, ide_info_file, output_groups):
    """Updates Python-specific output groups, returns false if not a Python target."""
    if not py_info_in_target(target) or _is_language_specific_proto_library(ctx, target, semantics):
        return False

    py_semantics = getattr(semantics, "py", None)
    if py_semantics:
        py_launcher = py_semantics.get_launcher(target, ctx)
    else:
        py_launcher = None

    sources = sources_from_target(ctx)
    to_build = get_py_info(target).transitive_sources
    args = getattr(ctx.rule.attr, "args", [])
    data_deps = getattr(ctx.rule.attr, "data", [])
    args = _do_starlark_string_expansion(ctx, "args", args, data_deps, tokenization = False)
    imports = getattr(ctx.rule.attr, "imports", [])
    is_code_generator = False

    # If there are apparently no sources found from `srcs` and the target has a rule name which is
    # one of the ones pre-specified to the aspect as being a code-generator for Python then
    # interpret the outputs of the target specified in the PyInfo as being sources.

    if 0 == len(sources) and ctx.rule.kind in get_code_generator_rule_names(ctx, "python"):
        def provider_import_to_attr_import(provider_import):
            """\
            Remaps the imports from PyInfo

            The imports that are supplied on the `PyInfo` are relative to the runfiles and so are
            not the same as those which might be supplied on an attribute of `py_library`. This
            function will remap those back so they look as if they were `imports` attributes on
            the rule. The form of the runfiles import is `<workspace_name>/<package_dir>/<import>`.
            The actual `workspace_name` is not interesting such that the first part can be simply
            stripped. Next the package to the Label is stripped leaving a path that would have been
            supplied on an `imports` attribute to a Rule.
            """

            # Other code in this file appears to assume *NIX path component separators?

            provider_import_parts = [p for p in provider_import.split("/")]
            package_parts = [p for p in ctx.label.package.split("/")]

            if 0 == len(provider_import_parts):
                return None

            scratch_parts = provider_import_parts[1:]  # remove the workspace name or _main

            for p in package_parts:
                if 0 != len(provider_import_parts) and scratch_parts[0] == p:
                    scratch_parts = scratch_parts[1:]
                else:
                    return None

            return "/".join(scratch_parts)

        def provider_imports_to_attr_imports():
            result = []

            for provider_import in get_py_info(target).imports.to_list():
                attr_import = provider_import_to_attr_import(provider_import)
                if attr_import:
                    result.append(attr_import)

            return result

        if get_py_info(target).imports:
            imports.extend(provider_imports_to_attr_imports())

        runfiles = target[DefaultInfo].default_runfiles

        if runfiles and runfiles.files:
            sources.extend([artifact_location(f) for f in runfiles.files.to_list()])

        is_code_generator = True

    ide_info["py_ide_info"] = struct_omit_none(
        launcher = py_launcher,
        python_version = _get_python_version(ctx),
        sources = sources,
        srcs_version = _get_python_srcs_version(ctx),
        args = args,
        imports = imports,
        is_code_generator = is_code_generator,
    )

    update_sync_output_groups(output_groups, "intellij-info-py", depset([ide_info_file]))
    update_sync_output_groups(output_groups, "intellij-compile-py", to_build)
    update_sync_output_groups(output_groups, "intellij-resolve-py", to_build)
    return True

def _collect_generated_go_sources(target, ctx, semantics):
    """Returns a depset of go source files generated by this target."""
    if semantics.go.is_proto_library(target, ctx):
        return semantics.go.get_proto_library_generated_srcs(target)
    else:
        return None

def collect_go_info(target, ctx, semantics, ide_info, ide_info_file, output_groups):
    """Updates Go-specific output groups, returns false if not a recognized Go target."""
    sources = []
    generated = []
    cgo = False

    # currently there's no Go Starlark API, with the only exception being proto_library targets
    if ctx.rule.kind in [
        "go_binary",
        "go_library",
        "go_test",
        "go_source",
        "go_appengine_binary",
        "go_appengine_library",
        "go_appengine_test",
    ]:
        sources = [f for src in getattr(ctx.rule.attr, "srcs", []) for f in src.files.to_list()]
        generated = [f for f in sources if not f.is_source]
        cgo = getattr(ctx.rule.attr, "cgo", False)
    elif ctx.rule.kind == "go_wrap_cc":
        genfiles = target.files.to_list()
        go_genfiles = [f for f in genfiles if f.basename.endswith(".go")]
        if go_genfiles:
            sources = go_genfiles
            generated = go_genfiles
        else:
            # if the .go file isn't in 'files', build the .a and .x files instead
            generated = genfiles
    else:
        generated_sources = _collect_generated_go_sources(target, ctx, semantics)
        if not generated_sources:
            return False
        sources = generated_sources
        generated = generated_sources

    import_path = None
    go_semantics = getattr(semantics, "go", None)
    if go_semantics:
        import_path = go_semantics.get_import_path(ctx)

    library_labels = []
    if ctx.rule.kind in ("go_test", "go_library", "go_appengine_test"):
        if getattr(ctx.rule.attr, "library", None) != None:
            library_labels = [stringify_label(ctx.rule.attr.library.label)]
        elif getattr(ctx.rule.attr, "embed", None) != None:
            for library in ctx.rule.attr.embed:
                if library[IntelliJInfo].kind == "go_source" or library[IntelliJInfo].kind == "go_proto_library":
                    l = library[IntelliJInfo].output_groups["intellij-sources-go-outputs"].to_list()
                    sources += l
                    generated += [f for f in l if not f.is_source]
                else:
                    library_labels.append(stringify_label(library.label))

    ide_info["go_ide_info"] = struct_omit_none(
        import_path = import_path,
        library_labels = library_labels,
        sources = [artifact_location(f) for f in sources],
        cgo = cgo,
    )

    compile_files = target[OutputGroupInfo].compilation_outputs if hasattr(target[OutputGroupInfo], "compilation_outputs") else depset([])
    compile_files = depset(generated, transitive = [compile_files])

    update_sync_output_groups(output_groups, "intellij-info-go", depset([ide_info_file]))
    update_sync_output_groups(output_groups, "intellij-compile-go", compile_files)
    update_sync_output_groups(output_groups, "intellij-resolve-go", depset(generated))
    update_sync_output_groups(output_groups, "intellij-sources-go", depset(sources))
    return True

def collect_cpp_info(target, ctx, semantics, ide_info, ide_info_file, output_groups):
    """Updates C++-specific output groups, returns false if not a C++ target."""

    if CcInfo not in target:
        return False

    # ignore cc_proto_library, attach to proto_library with aspect attached instead
    if ctx.rule.kind == "cc_proto_library":
        return False

    # Go targets always provide CcInfo. Usually it's empty, but even if it isn't we don't handle it
    if ctx.rule.kind.startswith("go_"):
        return False

    sources = artifacts_from_target_list_attr(ctx, "srcs")
    headers = artifacts_from_target_list_attr(ctx, "hdrs")
    textual_headers = artifacts_from_target_list_attr(ctx, "textual_hdrs")

    target_copts = []
    if hasattr(ctx.rule.attr, "copts"):
        target_copts += ctx.rule.attr.copts
    extra_targets = []
    if hasattr(ctx.rule.attr, "additional_compiler_inputs"):
        extra_targets += ctx.rule.attr.additional_compiler_inputs
    if hasattr(semantics, "cc") and hasattr(semantics.cc, "get_default_copts"):
        target_copts += semantics.cc.get_default_copts(ctx)

    target_copts = _do_starlark_string_expansion(ctx, "copt", target_copts, extra_targets)

    compilation_context = target[CcInfo].compilation_context

    # Merge current compilation context with context of implementation dependencies.
    if hasattr(ctx.rule.attr, "implementation_deps"):
        implementation_deps = ctx.rule.attr.implementation_deps
        compilation_context = cc_common.merge_compilation_contexts(
            compilation_contexts =
                [compilation_context] + [impl[CcInfo].compilation_context for impl in implementation_deps],
        )

    # external_includes available since bazel 7
    external_includes = getattr(compilation_context, "external_includes", depset()).to_list()

    c_info = struct_omit_none(
        header = headers,
        source = sources,
        target_copt = target_copts,
        textual_header = textual_headers,
        transitive_define = compilation_context.defines.to_list(),
        transitive_include_directory = compilation_context.includes.to_list(),
        transitive_quote_include_directory = compilation_context.quote_includes.to_list(),
        # both system and external includes are add using `-isystem`
        transitive_system_include_directory = compilation_context.system_includes.to_list() + external_includes,
        include_prefix = getattr(ctx.rule.attr, "include_prefix", None),
        strip_include_prefix = getattr(ctx.rule.attr, "strip_include_prefix", None),
    )
    ide_info["c_ide_info"] = c_info
    resolve_files = compilation_context.headers

    # TODO(brendandouglas): target to cpp files only
    compile_files = target[OutputGroupInfo].compilation_outputs if hasattr(target[OutputGroupInfo], "compilation_outputs") else depset([])

    update_sync_output_groups(output_groups, "intellij-info-cpp", depset([ide_info_file]))
    update_sync_output_groups(output_groups, "intellij-compile-cpp", compile_files)
    update_sync_output_groups(output_groups, "intellij-resolve-cpp", resolve_files)
    return True

def collect_c_toolchain_info(target, ctx, semantics, ide_info, ide_info_file, output_groups):
    """Updates cc_toolchain-relevant output groups, returns false if not a cc_toolchain target."""

    # The other toolchains like the JDK might also have ToolchainInfo but it's not a C++ toolchain,
    # so check kind as well.
    # TODO(jvoung): We are temporarily getting info from cc_toolchain_suite
    # https://github.com/bazelbuild/bazel/commit/3aedb2f6de80630f88ffb6b60795c44e351a5810
    # but will switch back to cc_toolchain providing CcToolchainProvider once we migrate C++ rules
    # to generic platforms and toolchains.
    if ctx.rule.kind != "cc_toolchain" and ctx.rule.kind != "cc_toolchain_suite" and ctx.rule.kind != "cc_toolchain_alias":
        return False
    if cc_common.CcToolchainInfo not in target:
        return False

    # cc toolchain to access compiler flags
    cpp_toolchain = target[cc_common.CcToolchainInfo]

    # cpp fragment to access bazel options
    cpp_fragment = ctx.fragments.cpp

    copts = cpp_fragment.copts
    cxxopts = cpp_fragment.cxxopts
    conlyopts = cpp_fragment.conlyopts

    feature_configuration = cc_common.configure_features(
        ctx = ctx,
        cc_toolchain = cpp_toolchain,
        requested_features = ctx.features,
        unsupported_features = ctx.disabled_features + UNSUPPORTED_FEATURES,
    )
    c_variables = cc_common.create_compile_variables(
        feature_configuration = feature_configuration,
        cc_toolchain = cpp_toolchain,
        user_compile_flags = copts + conlyopts,
    )
    cpp_variables = cc_common.create_compile_variables(
        feature_configuration = feature_configuration,
        cc_toolchain = cpp_toolchain,
        user_compile_flags = copts + cxxopts,
    )
    c_options = cc_common.get_memory_inefficient_command_line(
        feature_configuration = feature_configuration,
        action_name = ACTION_NAMES.c_compile,
        variables = c_variables,
    )
    cpp_options = cc_common.get_memory_inefficient_command_line(
        feature_configuration = feature_configuration,
        action_name = ACTION_NAMES.cpp_compile,
        variables = cpp_variables,
    )

    if CC_USE_GET_TOOL_FOR_ACTION:
        c_compiler = cc_common.get_tool_for_action(
            feature_configuration = feature_configuration,
            action_name = ACTION_NAMES.c_compile,
        )
        cpp_compiler = cc_common.get_tool_for_action(
            feature_configuration = feature_configuration,
            action_name = ACTION_NAMES.cpp_compile,
        )
    else:
        c_compiler = str(cpp_toolchain.compiler_executable)
        cpp_compiler = str(cpp_toolchain.compiler_executable)

    c_toolchain_info = struct_omit_none(
        built_in_include_directory = [str(d) for d in cpp_toolchain.built_in_include_directories],
        c_option = c_options,
        cpp_option = cpp_options,
        c_compiler = c_compiler,
        cpp_compiler = cpp_compiler,
        target_name = cpp_toolchain.target_gnu_system_name,
        compiler_name = cpp_toolchain.compiler,
        sysroot = cpp_toolchain.sysroot,
    )
    ide_info["c_toolchain_ide_info"] = c_toolchain_info
    update_sync_output_groups(output_groups, "intellij-info-cpp", depset([ide_info_file]))
    return True

def get_java_provider(target):
    """Find a provider exposing java compilation/outputs data."""

    # Check for kt providers before JavaInfo. e.g. kt targets have
    # JavaInfo, but their data lives in the "kt" provider and not JavaInfo.
    # See https://github.com/bazelbuild/intellij/pull/1202
    if hasattr(target, "kt") and hasattr(target.kt, "outputs"):
        return target.kt
    java_info = get_java_info(target)
    if java_info:
        return java_info
    return get_provider_from_target("JavaPluginInfo", target)

def _collect_generated_files(java):
    """Collects generated files from a Java target"""
    if hasattr(java, "java_outputs"):
        return [
            (outputs.generated_class_jar, outputs.generated_source_jar)
            for outputs in java.java_outputs
            if outputs.generated_class_jar != None
        ]
    return []

def collect_java_info(target, ctx, semantics, ide_info, ide_info_file, output_groups):
    """Updates Java-specific output groups, returns false if not a Java target."""
    java = get_java_provider(target)
    if not java:
        return False
    if hasattr(java, "java_outputs") and java.java_outputs:
        java_outputs = java.java_outputs
    elif hasattr(java, "outputs") and java.outputs:
        java_outputs = java.outputs.jars
    else:
        return False

    java_semantics = semantics.java if hasattr(semantics, "java") else None
    if java_semantics and java_semantics.skip_target(target, ctx):
        return False

    ide_info_files = []
    sources = sources_from_target(ctx)
    jars = [library_artifact(output, ctx.rule.kind) for output in java_outputs]
    class_jars = [output.class_jar for output in java_outputs if output and output.class_jar]
    output_jars = [jar for output in java_outputs for jar in jars_from_output(output)]
    resolve_files = output_jars
    compile_files = class_jars

    gen_jars = []
    for generated_class_jar, generated_source_jar in _collect_generated_files(java):
        gen_jars.append(annotation_processing_jars(generated_class_jar, generated_source_jar))
        resolve_files += [
            jar
            for jar in [
                generated_class_jar,
                generated_source_jar,
            ]
            if jar != None and not jar.is_source
        ]
        compile_files += [
            jar
            for jar in [generated_class_jar]
            if jar != None and not jar.is_source
        ]

    jdeps = None
    jdeps_file = None
    if java_semantics and hasattr(java_semantics, "get_filtered_jdeps"):
        jdeps_file = java_semantics.get_filtered_jdeps(target)
    if jdeps_file == None and hasattr(java, "outputs") and hasattr(java.outputs, "jdeps") and java.outputs.jdeps:
        jdeps_file = java.outputs.jdeps
    if jdeps_file:
        jdeps = artifact_location(jdeps_file)
        resolve_files.append(jdeps_file)

    java_sources, gen_java_sources, srcjars = divide_java_sources(ctx)

    if java_semantics:
        srcjars = java_semantics.filter_source_jars(target, ctx, srcjars)

    package_manifest = None
    if java_sources:
        package_manifest = build_java_package_manifest(ctx, target, java_sources, ".java-manifest")
        ide_info_files.append(package_manifest)

    filtered_gen_jar = None
    if java_sources and (gen_java_sources or srcjars):
        filtered_gen_jar, filtered_gen_resolve_files = _build_filtered_gen_jar(
            ctx,
            target,
            java_outputs,
            gen_java_sources,
            srcjars,
        )
        resolve_files += filtered_gen_resolve_files

    # Custom lint checks are incorporated as java plugins. We collect them here and register them with the IDE so that the IDE can also run the same checks.
    plugin_processor_jar_files = []
    if hasattr(ctx.rule.attr, "_android_lint_plugins"):
        plugin_processor_jar_files += [
            jar
            for p in getattr(ctx.rule.attr, "_android_lint_plugins", [])
            for jar in _android_lint_plugin_jars(p)
        ]

    if hasattr(java, "annotation_processing") and java.annotation_processing and hasattr(java.annotation_processing, "processor_classpath"):
        plugin_processor_jar_files += java.annotation_processing.processor_classpath.to_list()
    resolve_files += plugin_processor_jar_files
    plugin_processor_jars = [annotation_processing_jars(jar, None) for jar in depset(plugin_processor_jar_files).to_list()]

    java_info = struct_omit_none(
        filtered_gen_jar = filtered_gen_jar,
        generated_jars = gen_jars,
        jars = jars,
        jdeps = jdeps,
        main_class = getattr(ctx.rule.attr, "main_class", None),
        package_manifest = artifact_location(package_manifest),
        sources = sources,
        test_class = getattr(ctx.rule.attr, "test_class", None),
        plugin_processor_jars = plugin_processor_jars,
    )

    ide_info["java_ide_info"] = java_info
    ide_info_files.append(ide_info_file)
    update_sync_output_groups(output_groups, "intellij-info-java", depset(ide_info_files))
    update_sync_output_groups(output_groups, "intellij-compile-java", depset(compile_files))
    update_sync_output_groups(output_groups, "intellij-resolve-java", depset(resolve_files))

    # also add transitive hjars + src jars, to catch implicit deps
    if hasattr(java, "transitive_compile_time_jars"):
        update_set_in_dict(output_groups, "intellij-resolve-java-direct-deps", java.transitive_compile_time_jars)
        update_set_in_dict(output_groups, "intellij-resolve-java-direct-deps", java.transitive_source_jars)
    return True

def _android_lint_plugin_jars(target):
    java_info = get_java_info(target)
    if java_info:
        return java_info.transitive_runtime_jars.to_list()
    else:
        return []

def _package_manifest_file_argument(f):
    artifact = artifact_location(f)
    is_external = "1" if is_external_artifact(f.owner) else "0"
    return artifact.root_execution_path_fragment + "," + artifact.relative_path + "," + is_external

def build_java_package_manifest(ctx, target, source_files, suffix):
    """Builds the java package manifest for the given source files."""
    output = ctx.actions.declare_file(target.label.name + suffix)

    args = ctx.actions.args()
    args.add("--output_manifest")
    args.add(output.path)
    args.add_joined(
        "--sources",
        source_files,
        join_with = ":",
        map_each = _package_manifest_file_argument,
    )

    # Bazel has an option to put your command line args in a file, and then pass the name of that file as the only
    # argument to your executable. The PackageParser supports taking args in this way, we can pass in an args file
    # as "@filename".
    # Bazel Persistent Workers take their input as a file that contains the argument that will be parsed and turned
    # into a WorkRequest proto and read on stdin. It also wants an argument of the form "@filename". We can use the
    # params file as an arg file.
    # Thus if we always use a params file, we can support both persistent worker mode and local mode (regular) mode.
    args.use_param_file("@%s", use_always = True)
    args.set_param_file_format("multiline")

    run_jar(
        ctx = ctx,
        jar = ctx.file._package_parser,
        inputs = source_files,
        outputs = [output],
        arguments = [args],
        mnemonic = "JavaPackageManifest",
        progress_message = "Parsing java package strings for " + str(target.label),
        execution_requirements = {
            "supports-workers": "1",
            "requires-worker-protocol": "proto",
        },
    )
    return output

def _build_filtered_gen_jar(ctx, target, java_outputs, gen_java_sources, srcjars):
    """Filters the passed jar to contain only classes from the given manifest."""
    jar_artifacts = []
    source_jar_artifacts = []
    for jar in java_outputs:
        if jar.ijar:
            jar_artifacts.append(jar.ijar)
        elif jar.class_jar:
            jar_artifacts.append(jar.class_jar)
        if hasattr(jar, "source_jars") and jar.source_jars:
            source_jar_artifacts.extend(_list_or_depset_to_list(jar.source_jars))
        elif hasattr(jar, "source_jar") and jar.source_jar:
            source_jar_artifacts.append(jar.source_jar)

    if len(source_jar_artifacts) == 0 or len(jar_artifacts) == 0:
        jar_artifacts.extend([jar.class_jar for jar in java_outputs if jar.class_jar])

    filtered_jar = ctx.actions.declare_file(target.label.name + "-filtered-gen.jar")
    filtered_source_jar = ctx.actions.declare_file(target.label.name + "-filtered-gen-src.jar")
    args = []
    for jar in jar_artifacts:
        args += ["--filter_jar", jar.path]
    for jar in source_jar_artifacts:
        args += ["--filter_source_jar", jar.path]
    args += ["--filtered_jar", filtered_jar.path]
    args += ["--filtered_source_jar", filtered_source_jar.path]
    if gen_java_sources:
        for java_file in gen_java_sources:
            args += ["--keep_java_file", java_file.path]
    if srcjars:
        for source_jar in srcjars:
            args += ["--keep_source_jar", source_jar.path]
    run_jar(
        ctx = ctx,
        jar = ctx.file._jar_filter,
        inputs = jar_artifacts + source_jar_artifacts + gen_java_sources + srcjars,
        outputs = [filtered_jar, filtered_source_jar],
        arguments = args,
        mnemonic = "JarFilter",
        progress_message = "Filtering generated code for " + str(target.label),
    )
    output_jar = struct(
        jar = artifact_location(filtered_jar),
        source_jar = artifact_location(filtered_source_jar),
    )
    intellij_resolve_files = [filtered_jar, filtered_source_jar]
    return output_jar, intellij_resolve_files

def divide_java_sources(ctx):
    """Divide sources into plain java, generated java, and srcjars."""

    java_sources = []
    gen_java_sources = []
    srcjars = []
    if hasattr(ctx.rule.attr, "srcs"):
        srcs = ctx.rule.attr.srcs
        for src in srcs:
            for f in src.files.to_list():
                if f.basename.endswith(".java"):
                    if f.is_source:
                        java_sources.append(f)
                    else:
                        gen_java_sources.append(f)
                elif f.basename.endswith(".srcjar"):
                    srcjars.append(f)

    return java_sources, gen_java_sources, srcjars

def collect_android_info(target, ctx, semantics, ide_info, ide_info_file, output_groups):
    """Updates Android-specific output groups, returns true if any android specific info was collected."""
    handled = False
    handled = _collect_android_ide_info(target, ctx, semantics, ide_info, ide_info_file, output_groups) or handled
    handled = _collect_android_instrumentation_info(target, ctx, semantics, ide_info, ide_info_file, output_groups) or handled
    handled = _collect_aar_import_info(ctx, ide_info, ide_info_file, output_groups) or handled
    handled = _collect_android_sdk_info(ctx, ide_info, ide_info_file, output_groups) or handled

    if handled:
        # do this once do avoid adding unnecessary nesting to the depset
        # (https://docs.bazel.build/versions/master/skylark/performance.html#reduce-the-number-of-calls-to-depset)
        update_sync_output_groups(output_groups, "intellij-info-android", depset([ide_info_file]))
    return handled

def _collect_android_ide_info(target, ctx, semantics, ide_info, ide_info_file, output_groups):
    """Populates ide_info proto and intellij_resolve_android output group

    Updates ide_info proto with android_ide_info, and intellij_resolve_android with android
    resolve files. It returns false on android_library and android_binary targets, as this preserves
    consistent functionality with the previous condition of the presence of the .android legacy
    provider.
    """
    if ctx.rule.kind not in ["android_library", "android_binary", "kt_android_library"]:
        return False

    android_semantics = semantics.android if hasattr(semantics, "android") else None
    extra_ide_info = android_semantics.extra_ide_info(target, ctx) if android_semantics else {}

    if hasattr(android_common, "AndroidIdeInfo"):
        android = target[android_common.AndroidIdeInfo]
    else:
        # Backwards compatibility: supports android struct provider
        legacy_android = getattr(target, "android")

        # Transform into AndroidIdeInfo form
        android = struct(
            java_package = legacy_android.java_package,
            manifest = legacy_android.manifest,
            idl_source_jar = getattr(legacy_android.idl.output, "source_jar", None),
            idl_class_jar = getattr(legacy_android.idl.output, "class_jar", None),
            defines_android_resources = legacy_android.defines_resources,
            idl_import_root = getattr(legacy_android.idl, "import_root", None),
            resource_jar = legacy_android.resource_jar,
            signed_apk = legacy_android.apk,
            apks_under_test = legacy_android.apks_under_test,
        )

    output_jar = struct(
        class_jar = android.idl_class_jar,
        ijar = None,
        source_jar = android.idl_source_jar,
    ) if android.idl_class_jar else None

    resources = []
    res_folders = []
    resolve_files = jars_from_output(output_jar)
    if hasattr(ctx.rule.attr, "resource_files"):
        for artifact_path_fragments, res_files in get_res_artifacts(ctx.rule.attr.resource_files).items():
            # Generate unique ArtifactLocation for resource directories.
            root = to_artifact_location(*artifact_path_fragments)
            resources.append(root)

            # Generate aar
            aar_file_name = target.label.name.replace("/", "-")
            aar_file_name += "-" + str(hash(root.root_execution_path_fragment + root.relative_path + aar_file_name))

            aar = ctx.actions.declare_file(aar_file_name + ".aar")
            args = ctx.actions.args()

            # using param file to get around argument length limitation
            # the name of param file (%s) is automatically filled in by blaze
            args.use_param_file("@%s")
            args.set_param_file_format("multiline")

            args.add("--aar", aar)
            args.add("--manifest_file", android.manifest)
            args.add_joined("--resources", res_files, join_with = ",")
            args.add("--resource_root", root.relative_path if root.is_source else root.root_execution_path_fragment + "/" + root.relative_path)

            run_jar(
                ctx = ctx,
                jar = ctx.file._create_aar,
                outputs = [aar],
                inputs = [android.manifest] + res_files,
                arguments = [args],
                mnemonic = "CreateAar",
                progress_message = "Generating " + aar_file_name + ".aar for target " + str(target.label),
            )
            resolve_files.append(aar)

            # Generate unique ResFolderLocation for resource files.
            res_folders.append(struct_omit_none(aar = artifact_location(aar), root = root))

    instruments = None
    if hasattr(ctx.rule.attr, "instruments") and ctx.rule.attr.instruments:
        instruments = stringify_label(ctx.rule.attr.instruments.label)

    render_resolve_jar = None
    if android_semantics and hasattr(android_semantics, "build_render_resolve_jar"):
        render_resolve_jar = android_semantics.build_render_resolve_jar(target, ctx)

    if render_resolve_jar:
        update_sync_output_groups(output_groups, "intellij-render-resolve-android", depset([render_resolve_jar]))

    android_info = struct_omit_none(
        java_package = android.java_package,
        idl_import_root = getattr(android, "idl_import_root", None),
        manifest = artifact_location(android.manifest),
        manifest_values = [struct_omit_none(key = key, value = value) for key, value in ctx.rule.attr.manifest_values.items()] if hasattr(ctx.rule.attr, "manifest_values") else None,
        apk = artifact_location(android.signed_apk),
        dependency_apk = [artifact_location(apk) for apk in android.apks_under_test],
        has_idl_sources = android.idl_class_jar != None,
        idl_jar = library_artifact(output_jar),
        generate_resource_class = android.defines_android_resources,
        resources = resources,
        res_folders = res_folders,
        resource_jar = library_artifact(android.resource_jar),
        instruments = instruments,
        render_resolve_jar = artifact_location(render_resolve_jar) if render_resolve_jar else None,
        **extra_ide_info
    )

    if android.manifest and not android.manifest.is_source:
        resolve_files.append(android.manifest)

    # b/176209293: expose resource jar to make sure empty library
    # knows they are remote output artifact
    if android.resource_jar:
        resolve_files += [jar for jar in jars_from_output(android.resource_jar)]

    ide_info["android_ide_info"] = android_info
    update_sync_output_groups(output_groups, "intellij-resolve-android", depset(resolve_files))
    return True

def _collect_android_instrumentation_info(target, ctx, semantics, ide_info, ide_info_file, output_groups):
    """Updates ide_info proto with android_instrumentation_info, returns false if not an android_instrumentation_test target."""
    if not ctx.rule.kind == "android_instrumentation_test":
        return False

    android_instrumentation_info = struct_omit_none(
        test_app = stringify_label(ctx.rule.attr.test_app.label),
        target_device = str(ctx.rule.attr.target_device.label),
    )
    ide_info["android_instrumentation_info"] = android_instrumentation_info
    return True

def _collect_android_sdk_info(ctx, ide_info, ide_info_file, output_groups):
    """Updates android_sdk-relevant groups, returns false if not an android_sdk target."""
    if ctx.rule.kind != "android_sdk":
        return False
    android_jar_file = ctx.rule.attr.android_jar.files.to_list()[0]
    ide_info["android_sdk_ide_info"] = struct(
        android_jar = artifact_location(android_jar_file),
    )
    return True

def _collect_aar_import_info(ctx, ide_info, ide_info_file, output_groups):
    """Updates ide_info proto with aar_import-relevant groups, returns false if not an aar_import target."""
    if ctx.rule.kind != "aar_import":
        return False
    if not hasattr(ctx.rule.attr, "aar"):
        return False
    aar_file = ctx.rule.attr.aar.files.to_list()[0]
    ide_info["android_aar_ide_info"] = struct_omit_none(
        aar = artifact_location(aar_file),
        java_package = getattr(ctx.rule.attr, "package", None),
    )
    update_sync_output_groups(output_groups, "intellij-resolve-android", depset([aar_file]))
    return True

def build_test_info(ctx):
    """Build TestInfo."""
    if not is_test_rule(ctx):
        return None
    return struct_omit_none(
        size = ctx.rule.attr.size,
    )

def is_test_rule(ctx):
    kind_string = ctx.rule.kind
    return kind_string.endswith("_test")

def collect_java_toolchain_info(target, ide_info, ide_info_file, output_groups):
    """Updates java_toolchain-relevant output groups, returns false if not a java_toolchain target."""
    if hasattr(target, "java_toolchain"):
        toolchain = target.java_toolchain
    else:
        toolchain = get_provider_from_target("JavaToolchainInfo", target)
    if not toolchain:
        return False
    javac_jars = []
    if hasattr(toolchain, "tools"):
        javac_jars = [
            artifact_location(f)
            for f in toolchain.tools.to_list()
            if f.basename.endswith(".jar")
        ]
    ide_info["java_toolchain_ide_info"] = struct_omit_none(
        javac_jars = javac_jars,
        source_version = toolchain.source_version,
        target_version = toolchain.target_version,
    )
    update_sync_output_groups(output_groups, "intellij-info-java", depset([ide_info_file]))
    return True

def artifact_to_path(artifact):
    return artifact.root_execution_path_fragment + "/" + artifact.relative_path

def collect_kotlin_toolchain_info(target, ctx, ide_info, ide_info_file, output_groups):
    """Updates kotlin_toolchain-relevant output groups, returns false if not a kotlin_toolchain target."""
    if ctx.rule.kind == "_kt_toolchain" and platform_common.ToolchainInfo in target:
        kt = target[platform_common.ToolchainInfo]
    elif hasattr(target, "kt") and hasattr(target.kt, "language_version"):
        kt = target.kt  # Legacy struct provider mechanism
    else:
        return False

    if not hasattr(kt, "language_version"):
        return False
    ide_info["kt_toolchain_ide_info"] = struct(
        language_version = kt.language_version,
    )
    update_sync_output_groups(output_groups, "intellij-info-kt", depset([ide_info_file]))
    return True

def _is_proto_library_wrapper(target, ctx):
    """Returns True if the target is an empty shim around a proto library."""
    if not ctx.rule.kind.endswith("proto_library") or ctx.rule.kind == "proto_library":
        return False

    # treat any *proto_library rule with a single proto_library dep as a shim
    deps = collect_targets_from_attrs(ctx.rule.attr, ["deps"])
    return len(deps) == 1 and IntelliJInfo in deps[0] and deps[0][IntelliJInfo].kind == "proto_library"

def _get_forwarded_deps(target, ctx):
    """Returns the list of deps of this target to forward.

    Used to handle wrapper/shim targets which are really just pointers to a
    different target (for example, java_proto_library)
    """
    if _is_proto_library_wrapper(target, ctx):
        return collect_targets_from_attrs(ctx.rule.attr, ["deps"])
    return []

def _is_analysis_test(target):
    """Returns if the target is an analysis test.

    Rules created with analysis_test=True cannot create write actions, so the
    aspect should skip them.
    """
    return AnalysisTestResultInfo in target

##### Main aspect function

def intellij_info_aspect_impl(target, ctx, semantics):
    """Aspect implementation function."""

    tags = ctx.rule.attr.tags
    if "no-ide" in tags:
        return []

    if _is_analysis_test(target):
        return []

    rule_attrs = ctx.rule.attr

    # Collect direct dependencies
    direct_dep_targets = collect_targets_from_attrs(
        rule_attrs,
        semantics_extra_deps(DEPS, semantics, "extra_deps"),
    )

    # Collect direct toolchain type-based dependencies
    if hasattr(semantics, "toolchains_propagation"):
        direct_dep_targets.extend(
            semantics.toolchains_propagation.collect_toolchain_deps(
                ctx,
                semantics.toolchains_propagation.toolchain_types,
            ),
        )

    direct_deps = make_deps(direct_dep_targets, COMPILE_TIME)

    # Add exports from direct dependencies
    exported_deps_from_deps = []
    for dep in direct_dep_targets:
        exported_deps_from_deps = exported_deps_from_deps + dep[IntelliJInfo].export_deps

    # Combine into all compile time deps
    compiletime_deps = direct_deps + exported_deps_from_deps

    # Propagate my own exports
    export_deps = []
    direct_exports = []
    if java_info_in_target(target):
        direct_exports = collect_targets_from_attrs(rule_attrs, ["exports"])
        export_deps.extend(make_deps(direct_exports, COMPILE_TIME))

        # Collect transitive exports
        for export in direct_exports:
            export_deps.extend(export[IntelliJInfo].export_deps)

        if ctx.rule.kind == "android_library" or ctx.rule.kind == "kt_android_library":
            # Empty android libraries export all their dependencies.
            if not hasattr(rule_attrs, "srcs") or not ctx.rule.attr.srcs:
                export_deps.extend(compiletime_deps)

        # Deduplicate the entries
        export_deps = depset(export_deps).to_list()

    # runtime_deps
    runtime_dep_targets = collect_targets_from_attrs(
        rule_attrs,
        RUNTIME_DEPS,
    )
    runtime_deps = make_deps(runtime_dep_targets, RUNTIME)
    all_deps = depset(compiletime_deps + runtime_deps).to_list()

    # extra prerequisites
    extra_prerequisite_targets = collect_targets_from_attrs(
        rule_attrs,
        semantics_extra_deps(PREREQUISITE_DEPS, semantics, "extra_prerequisites"),
    )

    forwarded_deps = _get_forwarded_deps(target, ctx) + direct_exports

    # Roll up output files from my prerequisites
    prerequisites = direct_dep_targets + runtime_dep_targets + extra_prerequisite_targets + direct_exports
    output_groups = dict()
    for dep in prerequisites:
        for k, v in dep[IntelliJInfo].output_groups.items():
            if dep in forwarded_deps:
                # unconditionally roll up deps for these targets
                output_groups[k] = output_groups[k] + [v] if k in output_groups else [v]
                continue

            # roll up outputs of direct deps into '-direct-deps' output group
            if k.endswith("-direct-deps"):
                continue
            if k.endswith("-outputs"):
                directs = k[:-len("outputs")] + "direct-deps"
                output_groups[directs] = output_groups[directs] + [v] if directs in output_groups else [v]
                continue

            # everything else gets rolled up transitively
            output_groups[k] = output_groups[k] + [v] if k in output_groups else [v]

    # Convert output_groups from lists to depsets after the lists are finalized. This avoids
    # creating and growing depsets gradually, as that results in depsets many levels deep:
    # a construct which would give the build system some trouble.
    for k, v in output_groups.items():
        output_groups[k] = depset(transitive = output_groups[k])

    # Initialize the ide info dict, and corresponding output file
    # This will be passed to each language-specific handler to fill in as required
    file_name = target.label.name

    # bazel allows target names differing only by case, so append a hash to support
    # case-insensitive file systems
    file_name = file_name + "-" + str(hash(file_name))
    aspect_ids = get_aspect_ids(ctx)
    if aspect_ids:
        aspect_hash = hash(".".join(aspect_ids))
        file_name = file_name + "-" + str(aspect_hash)
    file_name = file_name + ".intellij-info.txt"
    ide_info_file = ctx.actions.declare_file(file_name)

    target_key = make_target_key(target.label, aspect_ids)
    ide_info = dict(
        build_file_artifact_location = build_file_artifact_location(ctx),
        features = ctx.features,
        key = target_key,
        kind_string = ctx.rule.kind,
        tags = tags,
        deps = list(all_deps),
    )

    # Collect test info
    ide_info["test_info"] = build_test_info(ctx)

    handled = False
    handled = collect_py_info(target, ctx, semantics, ide_info, ide_info_file, output_groups) or handled
    handled = collect_cpp_info(target, ctx, semantics, ide_info, ide_info_file, output_groups) or handled
    handled = collect_c_toolchain_info(target, ctx, semantics, ide_info, ide_info_file, output_groups) or handled
    handled = collect_go_info(target, ctx, semantics, ide_info, ide_info_file, output_groups) or handled
    handled = collect_java_info(target, ctx, semantics, ide_info, ide_info_file, output_groups) or handled
    handled = collect_java_toolchain_info(target, ide_info, ide_info_file, output_groups) or handled
    handled = collect_android_info(target, ctx, semantics, ide_info, ide_info_file, output_groups) or handled
    handled = collect_kotlin_toolchain_info(target, ctx, ide_info, ide_info_file, output_groups) or handled

    # Any extra ide info
    if hasattr(semantics, "extra_ide_info"):
        handled = semantics.extra_ide_info(target, ctx, ide_info, ide_info_file, output_groups) or handled

    # Add to generic output group if it's not handled by a language-specific handler
    if not handled:
        update_sync_output_groups(output_groups, "intellij-info-generic", depset([ide_info_file]))

    # Output the ide information file.
    info = struct_omit_none(**ide_info)
    ctx.actions.write(ide_info_file, proto.encode_text(info))

    # Return providers.
    return [
        IntelliJInfo(
            export_deps = export_deps,
            kind = ctx.rule.kind,
            output_groups = output_groups,
            target_key = target_key,
        ),
        OutputGroupInfo(**output_groups),
    ]

def semantics_extra_deps(base, semantics, name):
    if not hasattr(semantics, name):
        return base
    extra_deps = getattr(semantics, name)
    return base + extra_deps

def make_intellij_info_aspect(aspect_impl, semantics, **kwargs):
    """Creates the aspect given the semantics."""
    tool_label = semantics.tool_label
    deps = semantics_extra_deps(DEPS, semantics, "extra_deps")
    runtime_deps = RUNTIME_DEPS
    prerequisite_deps = semantics_extra_deps(PREREQUISITE_DEPS, semantics, "extra_prerequisites")

    attr_aspects = deps + runtime_deps + prerequisite_deps

    attrs = {
        "_package_parser": attr.label(
            default = tool_label("PackageParser_deploy.jar"),
            allow_single_file = True,
        ),
        "_jar_filter": attr.label(
            default = tool_label("JarFilter_deploy.jar"),
            allow_single_file = True,
        ),
        "_create_aar": attr.label(
            default = tool_label("CreateAar_deploy.jar"),
            allow_single_file = True,
        ),
        "_java_runtime": attr.label(
            default = "@bazel_tools//tools/jdk:current_java_runtime",
            cfg = "exec",
        ),
    }

    # add attrs required by semantics
    if hasattr(semantics, "attrs"):
        attrs.update(semantics.attrs)

    return aspect(
        attr_aspects = attr_aspects,
        attrs = attrs,
        fragments = ["cpp"],
        required_aspect_providers = [java_info_reference(), [CcInfo]] + semantics.extra_required_aspect_providers,
        implementation = aspect_impl,
        **kwargs
    )
