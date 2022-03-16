---
title: Cloud operation costs
sidebar_position: 1
---

Quickwit has been tested on Amazon S3. This page sums up what we have learned from that experience.

## Data transfers costs and latency

Cloud providers charge for data transfers in and out of their networks. In addition, querying an index from a remote machine adds some extra latency.
For those reasons, we recommend that you test and use the Quickwit from an instance located within your cloud provider's network.

## Optimizing bandwidth with wisely chosen instances

We recommend picking instances with high network performance to allow faster downloads from Amazon S3. In our experience, `c5n.2xlarge` instances offer the best bang for your buck.

## Requests cost

A final note on object storage requests costs. These are [quite low](https://aws.amazon.com/s3/pricing/) actually, $0,0004 / 1000 requests for GET and $0.005 / 1000 requests for PUT on AWS S3.

### PUT requests

During indexing, Quickwit uploads new splits on Amazon S3 and progressively merges them until they reach 10 million documents that we call “mature splits”. Such splits have a typical size between 1GB and 10GB and will usually require 2 PUT requests to be uploaded (1 PUT request / 5GB).

With default indexing parameters `commit_timeout_secs` of 60 seconds and `merge_policy.merge_factor` of 10 and assuming you want to ingest 1 million documents every minute, this will cost you less than $1 / month.

### GET requests

When querying, Quickwit needs to make multiple GET requests:

```jsx
#num requests = #num splits * ((#num search fields * #num terms * 3) + 1 (timestamp fast field if present))
```

The above formula assumes that the hotcache is cached, which will be loaded after the first query for every split.

When positions are not enabled, only 2 GET requests will be executed per term.

These requests costs could add up quickly if you have a high number of splits or QPS > 10.
Don't hesitate to [contact us](mailto:hello@quickwit.io) if this is the case :).
