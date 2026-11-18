# Requested postings ranges

Two datasets contain the unmerged postings byte ranges for the same six queries
from [the inline-postings experiment](https://github.com/PSeitz/tantivy/blob/sstable_regex/experiments/inline-postings.md#3-external-postings-read-plans):

| File | Layout | Requested ranges |
|---|---|---:|
| `requested_ranges.json` | Inline lists with at most eight documents | 267 |
| `requested_ranges_no_inline.json` | Original postings, without inlining | 35,608 |

Each pair is `[start, end)` in the same postings file. Query lists are independent
inputs to the storage coalescer's range planner. Both fixtures contain query labels and byte offsets.
Each query keeps its first 16,384 external ranges in dictionary order. All queries
in the inline fixture already fit within this limit. This prefix selection favors
the beginning of the dictionary and changes the no-inlining query totals.
Both fixtures are plain JSON with the same schema. The no-inlining fixture is
about 752 KiB.

## Source and extraction

- Split: `KH4Y_01M5MTJQ3SVHDTK2W3XCB5`.
- Segment: `4fea6c896fd045aba3cece0808f3e41e`.
- Field: 3 (`message`). Offsets come from the segment's `.term` dictionary.

The no-inlining fixture keeps original postings offsets. The inline fixture removes
all lists with `doc_freq <= 8`, including nonmatching terms, and shifts subsequent
offsets by the removed bytes. Ranges remain unmerged.

Queries use case-sensitive byte matching. Measurements simulate postings reads in
these two layouts.

## Reference measurements

The planner merges gaps up to 4 MiB. This is a per-gap threshold, not a download-size
or total-gap-byte limit. Each policy cell shows **downloads / downloaded bytes**.
Requested bytes count only the ranges in the fixture. Downloaded bytes include gaps.
The plans below were calculated independently in Python and checked against Rust.

### Inline cutoff eight

| Query | Requested ranges | Requested bytes | 4 MiB gaps |
|---|---:|---:|---:|
| `*ab*` | 203 | 288,249 | 9 / 38,242,633 |
| `*aba*` | 16 | 143,930 | 2 / 7,888,734 |
| `*traaa*` | 0 | 0 | 0 / 0 |
| `traaa*` | 0 | 0 | 0 / 0 |
| `*player*` | 21 | 4,221,761 | 6 / 15,382,086 |
| `*ms*` | 27 | 825,018 | 8 / 19,690,015 |

### Without inlining

| Query | Requested ranges | Requested bytes | 4 MiB gaps |
|---|---:|---:|---:|
| `*ab*` | 16,384 | 82,276 | 2 / 107,392,855 |
| `*aba*` | 2,817 | 157,561 | 4 / 237,355,128 |
| `*traaa*` | 2 | 10 | 2 / 10 |
| `traaa*` | 0 | 0 | 0 / 0 |
| `*player*` | 21 | 4,221,761 | 6 / 17,345,872 |
| `*ms*` | 16,384 | 200,211 | 2 / 159,383,608 |

Run both fixture tests and print the download counts and bytes from `quickwit/`:

```sh
cargo test --locked -p quickwit-storage test_coalesce_ranges_from_postings -- --nocapture
```

Use the same inputs to compare alternative policies. Keep request count and
fetched bytes separate: reducing requests can increase fetched bytes. These fixtures
cover external postings with and without inlining. Dictionary reads, positions,
timing, cache reuse, and concurrent queries require additional measurements.
