---
title: Operating in the cloud
sidebar_position: 1
---

Quickwit has been only tested on AWS S3. This page sums up what we have learned from that.


## Data transfers costs and latency

Cloud providers charge for data transfers in and out of their networks. In addition, querying an index from a remote machine adds some extra latency. For those reasons, we recommend that you test and use the Quickwit from an instance located within your cloud provider's network.

## Optimizing bandwith with wisely choosen instances

To fully benefit from Quickwit search from object storage, the highest the bandwidth is between AWS S3 and your instance, the fastest Quickwit is. To get the most of it, we recommend you to use instances that maximises the bandwidth such as `c5n.2xlarge` instance.


## GET/PUT requests costs

A final note on object storage requests costs. These are [quite low](https://aws.amazon.com/s3/pricing/) actually, $0,0004 / 1000 requests for GET and $0.005 / 1000 requests for PUT on AWS S3 so you don't need to worry much about it. 
Indeed when indexing, Quickwit generates [splits](../overview/architecture.md#splits) of 5 millions documents each and then 
upload them. As they are composed of 9 files, this generates 9 PUT requests per split.
When querying, Quickwit only needs to make 3 GET requests per split.

Of course, these requests can add up quickly on specific use cases. Don't hesite to [contact us](mailto:hello@quickwit.io) if this is the case :).
