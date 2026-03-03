"""Utility methods for working with ArtifactLocation types."""

def struct_omit_none(**kwargs):
    """A replacement for standard `struct` function that omits the fields with None value."""
    d = {name: kwargs[name] for name in kwargs if kwargs[name] != None}
    return struct(**d)

def sources_from_target(ctx):
    """Get the list of sources from a target as artifact locations."""
    return artifacts_from_target_list_attr(ctx, "srcs")

def artifacts_from_target_list_attr(ctx, attr_name):
    """Converts a list of targets to a list of artifact locations."""
    return [
        artifact_location(f)
        for target in getattr(ctx.rule.attr, attr_name, [])
        for f in target.files.to_list()
    ]

def artifact_location(f):
    """Creates an ArtifactLocation proto from a File."""
    if f == None:
        return None

    return to_artifact_location(
        f.path,
        f.root.path if not f.is_source else "",
        f.is_source,
        is_external_artifact(f.owner),
    )

def to_artifact_location(exec_path, root_exec_path_fragment, is_source, is_external):
    """Derives workspace path from other path fragments, and creates an ArtifactLocation proto."""

    # directory structure:
    # exec_path = (../repo_name)? + (root_fragment)? + relative_path
    relative_path = _strip_external_workspace_prefix(exec_path)
    relative_path = _strip_root_exec_path_fragment(relative_path, root_exec_path_fragment)

    root_exec_path_fragment = exec_path[:-(len("/" + relative_path))]

    return struct_omit_none(
        relative_path = relative_path,
        is_source = is_source,
        is_external = is_external,
        root_execution_path_fragment = root_exec_path_fragment,
    )

def is_external_artifact(label):
    """Determines whether a label corresponds to an external artifact."""

    # Label.EXTERNAL_PATH_PREFIX is due to change from 'external' to '..' in Bazel 0.4.5.
    # This code is for forwards and backwards compatibility.
    # Remove the 'external' check when Bazel 0.4.4 and earlier no longer need to be supported.
    return label.workspace_root.startswith("external") or label.workspace_root.startswith("..")

def _strip_root_exec_path_fragment(path, root_fragment):
    if root_fragment and path.startswith(root_fragment + "/"):
        return path[len(root_fragment + "/"):]
    return path

def _strip_external_workspace_prefix(path):
    """Either 'external/workspace_name/' or '../workspace_name/'."""

    # Label.EXTERNAL_PATH_PREFIX is due to change from 'external' to '..' in Bazel 0.4.5.
    # This code is for forwards and backwards compatibility.
    # Remove the 'external/' check when Bazel 0.4.4 and earlier no longer need to be supported.
    if path.startswith("../") or path.startswith("external/"):
        return "/".join(path.split("/")[2:])
    return path
