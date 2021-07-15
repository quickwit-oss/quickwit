---
title: Operating in the cloud
sidebar_position: 1
---

Quickwit has only been tested on AWS S3. This page sums up what we have learned from that.

## Data transfers costs and latency

Cloud providers charge for data transfers in and out of their networks. In addition, querying an index from a remote machine adds some extra latency. For those reasons, we recommend that you test and use the Quickwit from an instance located within your cloud provider's network.

## Optimizing bandwidth with wisely chosen instances

To get the best performance out of Quickwit search from object storage, we recommend picking an instance with high network bandwidth.
In our experience, `c5n.2xlarge` instances offer the bigger bang for your buck.

## GET/PUT requests costs

A final note on object storage requests costs. These are [quite low](https://aws.amazon.com/s3/pricing/) actually, $0,0004 / 1000 requests for GET and $0.005 / 1000 requests for PUT on AWS S3, so you don't need to worry too much about it.

Indeed when indexing, Quickwit generates [splits](../overview/architecture.md#splits) of 5 millions documents each and then
upload them. As they are composed of 9 files, this generates 9 PUT requests per split. When querying one term, Quickwit only needs to make 3 GET requests per split.

Of course, these requests could add up quickly if you have a large amount of requests.
Don't hesitate to [contact us](mailto:hello@quickwit.io) if this is the case :).
