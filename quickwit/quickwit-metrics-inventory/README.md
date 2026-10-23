# quickwit-metrics-inventory

Lists every metric declared with `quickwit-metrics` across the workspace crates.
The inventory is committed in [`metrics.yaml`](metrics.yaml), sorted by module
then metric:

```yaml
quickwit_cluster::metrics:
  quickwit_cluster_live_nodes:
    kind: gauge
    description: The number of live nodes observed locally.
```

## Updating the inventory

Contributors are responsible for regenerating and committing `metrics.yaml`
whenever they add, remove, or change a metric. From the repository root:

```bash
make metrics-inventory
git diff -- quickwit/quickwit-metrics-inventory/metrics.yaml
```

Do not run the binary directly with `cargo run`: it would only see metrics from
the crates listed in its own `Cargo.toml`. The script temporarily adds every
crate that depends on `quickwit-metrics`, builds them with `--all-features` so
that feature-gated metrics are included, then restores the modified files.

## CI check

The **Metrics inventory** workflow (`.github/workflows/metrics-inventory.yml`)
regenerates the inventory and:

- shows a `diff -u` against the committed `metrics.yaml` when it is out of date;
- warns about every metric present in the base revision's `metrics.yaml` but
  missing from the generated inventory, so reviewers can confirm the removal is
  intentional. Removals are detected whether or not `metrics.yaml` was updated.

The check is advisory: it never fails the build, and generation failures are
reported as an incomplete check. Do not make it a required status check.

Only metrics declared through `quickwit-metrics` are covered; this is not a
scrape of the `/metrics` endpoint.
