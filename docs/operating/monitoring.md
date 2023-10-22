---
title: Monitoring with Grafana
sidebar_position: 2
---

You can monitor your Quickwit cluster with Grafana.

We provide two Grafana dashboards to help you monitor:
- [indexers performance](https://github.com/quickwit-oss/quickwit/blob/main/monitoring/grafana/dashboards/indexers.json)
- [indexers performance](https://github.com/quickwit-oss/quickwit/blob/main/monitoring/grafana/dashboards/searchers.json)
- [metastore queries](https://github.com/quickwit-oss/quickwit/blob/main/monitoring/grafana/dashboards/metastore.json)

Both dashboards relies on a prometheus datasource fed with [Quickwit metrics](../reference/metrics.md).

## Screenshots

![Indexers Grafana Dashboard](../assets/images/screenshot-indexers-grafana-dashboard.jpeg)


![Searchers Grafana Dashboard](../assets/images/screenshot-searchers-grafana-dashboard.jpeg)

![Metastore Grafana Dashboard](../assets/images/screenshot-metastore-grafana-dashboard.png)
