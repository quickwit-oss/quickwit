#!/bin/bash

python3 - <<'PY'
import itertools
import os
from pathlib import Path
import re
import subprocess
import sys

expected_header = subprocess.check_output(["sed", r"s/{\\d+}/2021/", ".license_header.txt"])
tracked_files = subprocess.check_output(["git", "ls-files", "-z"]).split(b"\0")
result = 0
for tracked_file in tracked_files:
    filename = os.fsdecode(tracked_file)
    if not re.search(r"build|src|proto", filename):
        continue
    if not re.search(r"\.proto|\.rs|\.ts", filename):
        continue
    if any(excluded in filename for excluded in (
        "quickwit-proto/protos/third-party", "quickwit-proto/src", "/codegen/"
    )):
        continue
    path = Path(filename)
    if not path.exists():
        continue
    with path.open("rb") as source:
        if b"".join(itertools.islice(source, 14)) == expected_header:
            continue
        source.seek(0)
        if b"begin quickwit-codegen" in source.read().lower():
            continue
    print(f"Incomplete or missing license header in {filename}")
    result = 1
sys.exit(result)
PY
