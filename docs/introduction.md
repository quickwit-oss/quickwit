---
title: What is Quickwit?
slug: /
sidebar_position: 1
---

Quickwit is a distributed search engine built from the ground up to offer cost-efficiency and high reliability. By mere mortals for mere mortals, Quickwit's architecture is as simple as possible[^1].

Quickwit is written in Rust and built on top of the mighty [tantivy](https://github.com/tantivy-search/tantivy) library. We designed it to index large datasets.

## Why Quickwit?

Quickwit is born from the idea that today's search engines are hard to manage and uneconomical when dealing with large datasets and a low QPS[^2] rate. Its benefits are most apparent in a multitenancy or a multi-index setting.

Quickwit allows true decoupled compute and storage.
We designed it to search straight from object storage like Amazon S3 in a stateless manner.

Imagine hosting an arbitrary amount of indexes on Amazon S3 for $25 per TB/month and querying them with the same pool of search servers and with a subsecond latency.

Not only is Quickwit more cost-efficient, but search clusters are also easier to operate. One can add or remove search instances in seconds. You can also effortlessly index a massive amount of historical data using your favorite batch technology. Last but not least, Multi-tenant search is now cheap and painless.

- [Take a look at the feature set](overview/features.md)
- [Get started](getting-started/quickstart.md)


---
[^1] ... But not one bit simpler.

[^2] QPS stands for Queries per second. It is a standard measure of the amount of search traffic.
