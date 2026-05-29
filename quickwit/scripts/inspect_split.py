#!/usr/bin/env python3
"""Inspect a Quickwit `.split` bundle and print per-file sizes.

A split is laid out from the tail as:
    [ ... file data ... ]
    [ 8 bytes: magic + version (BundleStorageFileOffsetsVersions) ]
    [ JSON: {"files": {path: {"start": .., "end": ..}, ...}} ]
    [ 4 bytes LE u32: bundle metadata length (covers 8B header + JSON) ]
    [ ... hotcache bytes ... ]
    [ 4 bytes LE u32: hotcache length ]

See `BundleStorage::open_from_split_data` in
`quickwit/quickwit-storage/src/bundle_storage.rs` for the authoritative reader.
"""

import argparse
import json
import struct
import sys
from pathlib import Path

FOOTER_LEN_BYTES = 4
BUNDLE_HEADER_BYTES = 8  # magic (u32 LE) + version (u32 LE)


def inspect(path: Path) -> dict:
    with path.open("rb") as f:
        # Read hotcache length (last 4 bytes).
        f.seek(-FOOTER_LEN_BYTES, 2)
        (hot_len,) = struct.unpack("<I", f.read(FOOTER_LEN_BYTES))

        # Read bundle metadata length (4 bytes preceding the hotcache).
        f.seek(-(FOOTER_LEN_BYTES + hot_len + FOOTER_LEN_BYTES), 2)
        (meta_len,) = struct.unpack("<I", f.read(FOOTER_LEN_BYTES))

        # Read bundle metadata (the 8-byte magic+version header followed by JSON).
        f.seek(-(FOOTER_LEN_BYTES + hot_len + FOOTER_LEN_BYTES + meta_len), 2)
        meta = f.read(meta_len)

    if len(meta) < BUNDLE_HEADER_BYTES:
        raise RuntimeError(f"truncated bundle metadata ({len(meta)} bytes)")
    json_bytes = meta[BUNDLE_HEADER_BYTES:]
    files = json.loads(json_bytes)["files"]
    return {
        "total_size": path.stat().st_size,
        "hotcache_size": hot_len,
        "bundle_metadata_size": meta_len,
        "files": files,
    }


def format_size(n: int) -> str:
    units = [("GiB", 1024**3), ("MiB", 1024**2), ("KiB", 1024)]
    for label, scale in units:
        if n >= scale:
            return f"{n / scale:.2f} {label}"
    return f"{n} B"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("split", type=Path, help="path to a .split file")
    parser.add_argument(
        "--json", action="store_true", help="emit raw JSON instead of a table"
    )
    args = parser.parse_args()

    info = inspect(args.split)

    if args.json:
        print(json.dumps(info, indent=2, sort_keys=True))
        return 0

    files = info["files"]
    rows = sorted(
        ((name, rng["end"] - rng["start"]) for name, rng in files.items()),
        key=lambda kv: kv[1],
        reverse=True,
    )
    total = info["total_size"]
    formatted = [
        (name, format_size(size), f"{size:,}", f"{(size / total) * 100:.2f}%")
        for name, size in rows
    ]

    name_w = max(len("file"), *(len(name) for name, _, _, _ in formatted))
    size_w = max(len("size"), *(len(s) for _, s, _, _ in formatted))
    bytes_w = max(len("bytes"), *(len(b) for _, _, b, _ in formatted))
    pct_w = max(len("size %"), *(len(p) for _, _, _, p in formatted))

    summary = [
        ("total size", total),
        ("hotcache", info["hotcache_size"]),
        ("bundle metadata", info["bundle_metadata_size"]),
    ]
    summary_formatted = [
        (
            label,
            format_size(size),
            f"{size:,} B",
            f"{(size / total) * 100:.2f}%",
        )
        for label, size in summary
    ]
    label_w = max(len(label) for label, _, _, _ in summary_formatted) + 1  # +1 for ':'
    sum_size_w = max(len(s) for _, s, _, _ in summary_formatted)
    sum_bytes_w = max(len(b) for _, _, b, _ in summary_formatted)
    sum_pct_w = max(len(p) for _, _, _, p in summary_formatted)

    print(f"split: {args.split}")
    print()
    for label, size_str, bytes_str, pct_str in summary_formatted:
        print(
            f"{label + ':':<{label_w}}  {size_str:>{sum_size_w}}  "
            f"{bytes_str:>{sum_bytes_w}}  {pct_str:>{sum_pct_w}}"
        )
    print()
    print(
        f"{'file':<{name_w}}  {'size':>{size_w}}  {'bytes':>{bytes_w}}  {'size %':>{pct_w}}"
    )
    print(f"{'-' * name_w}  {'-' * size_w}  {'-' * bytes_w}  {'-' * pct_w}")
    for name, size_str, bytes_str, pct_str in formatted:
        print(
            f"{name:<{name_w}}  {size_str:>{size_w}}  {bytes_str:>{bytes_w}}  {pct_str:>{pct_w}}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
