import json
import subprocess
import sys


def has_automatic_fix(diagnostic):
    for span in diagnostic.get("spans", []):
        if (
            span.get("suggestion_applicability") == "MachineApplicable"
            and span.get("suggested_replacement") is not None
        ):
            return True
    return any(has_automatic_fix(child) for child in diagnostic.get("children", []))


def main():
    command = ["cargo", "clippy", "--workspace", "--all-features", "--tests"]
    needs_fix = False
    print("Checking for automatic lint fixes", flush=True)
    with subprocess.Popen(
        command + ["--no-deps", "--message-format=json"],
        stdout=subprocess.PIPE,
        text=True,
    ) as process:
        for line in process.stdout:
            if not line.startswith("{"):
                print(line, end="", flush=True)
                continue
            message = json.loads(line)
            if message.get("reason") != "compiler-message":
                continue
            diagnostic = message["message"]
            rendered = diagnostic.get("rendered")
            if rendered:
                print(rendered, end="", file=sys.stderr, flush=True)
            else:
                print(diagnostic["message"], file=sys.stderr, flush=True)
            needs_fix = has_automatic_fix(diagnostic) or needs_fix
        returncode = process.wait()

    if needs_fix:
        print("Applying automatic lint fixes", flush=True)
        return subprocess.run(
            command + ["--fix", "--allow-dirty", "--allow-staged"]
        ).returncode
    if returncode == 0:
        print("No automatic lint fixes needed", flush=True)
    return returncode


if __name__ == "__main__":
    sys.exit(main())
