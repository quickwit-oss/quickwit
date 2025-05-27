#!/usr/bin/env python3
import subprocess
import shutil
import os
from pathlib import Path
import sys

# ——— Configuration ———
REPOS = [
    "https://github.com/DataDog/integrations-core.git",
    "https://github.com/DataDog/integrations-extras.git",
    "https://github.com/DataDog/integrations-internal-core.git",
    "https://github.com/DataDog/marketplace.git",
]
CLONE_DIR = Path("tmp_repos")
OUTPUT_DIR = Path("integrations")
# ————————————————————

def setup_dirs():
    if CLONE_DIR.exists():
        shutil.rmtree(CLONE_DIR)
    CLONE_DIR.mkdir(parents=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def clone_repos():
    for url in REPOS:
        name = url.rstrip("/").split("/")[-1].removesuffix(".git")
        dest = CLONE_DIR / name
        print(f"Cloning {url} → {dest}")
        subprocess.run(
            ["git", "clone", "--depth=1", url, str(dest)],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

def copy_yaml_assets():
    processed = {}  # basename -> first-seen relative path
    count = 0
    cwd = Path.cwd()
    for repo_dir in CLONE_DIR.iterdir():
        print(f"Scanning {repo_dir} for assets/logs/*.yaml …")
        for src in repo_dir.rglob("assets/logs/*.yaml"):
            if not src.is_file():
                continue
            name = src.name
            src_rel = os.path.relpath(src, cwd)

            if name in processed:
                print(f"\nERROR: Duplicate filename detected: '{name}'")
                print(f"  First seen at: {processed[name]}")
                print(f"  Also found at: {src_rel}")
                print("Aborting, since we don't know which one is the right integration.")
                sys.exit(1)

            # record and copy
            processed[name] = src_rel
            dest = OUTPUT_DIR / name
            shutil.copy2(src, dest)
            dest_rel = os.path.relpath(dest, cwd)
            print(f"  Copied: {src_rel} → {dest_rel}")
            count += 1

    return count

def cleanup():
    print(f"Removing clone directory {CLONE_DIR}")
    shutil.rmtree(CLONE_DIR)

def main():
    setup_dirs()
    clone_repos()
    total = copy_yaml_assets()
    cleanup()
    print(f"\nDone! Total files copied: {total}")

if __name__ == "__main__":
    main()

