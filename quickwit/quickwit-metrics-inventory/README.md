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
regenerates the inventory and **fails** if it differs from the committed
`metrics.yaml`. The comparison is done by `scripts/check.sh`, which shows the
`diff -u` and classifies the changes.

To fix it, run `make metrics-inventory` and commit the result:

- **Added metrics or changed descriptions**: regenerating the inventory is
  enough.
- **Removed or renamed metrics, changed labels, or changed types (kind)**:
  these can break users of the metric. Regenerate the inventory, but be very
  cautious: identify every user of the affected metrics (dashboards, monitors,
  alerts, SLOs, documentation, downstream consumers), assess the impact, and
  communicate the change before merging.

Labels are compared using the static labels recorded in the inventory; labels
added at runtime are not part of the inventory.

Only metrics declared through `quickwit-metrics` are covered; this is not a
scrape of the `/metrics` endpoint.
