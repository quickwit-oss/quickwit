---
title: Open source search engines landscape
sidebar_position: 3
---

When starting Quickwit, we looked at the available solutions in the open-source world.
We also looked at the more fundamental brick that is the backbone of a search engine, the
algorithm that writes and reads efficient an inverted index or other related data structures to 
ensure high performance both at indexing and at searching.

Let's start with this fundamental brick and call it a search engine resusable library. As of today, there
is no doubt that [Lucene](https://lucene.apache.org/core/) is the reference, its dominance in 
the search domain is unquestionable, its performances are excellent, the code is well written and finally 
its pace of develpment is still impressive today. In other words, we can say that at Quickwit, we
love this library and it's actually a great source of inspiration at Quickwit.

So why bothering creating yet another search engine? Well, because 4 years ago,
[fulmicoton](https://fulmicoton.com/) was convinced there was a place for a high performance 
search library (in rust obviously) that may one time be comparable to the great 
[Lucene](https://lucene.apache.org/core/): that's how [tantivy](https://github.com/tantivy-search/tantivy)
was born and is now recognized as a serious and friendly competitor of Lucene. Have a look at
[tantivy benchmark](https://github.com/tantivy-search/search-benchmark-game).

As [tantivy](https://github.com/tantivy-search/tantivy) matures and gains popularity, 
the next obvious question was: can we bring something new that no other search engines has?

But the landscape of search engines (built for end user = not librairies) is more complexe than the one
of search libraries and have few very popular and robust search engine, we will stick to general purpose engines as
it would too hard to reference every search related project. The most famous ones are:
- [Elasticsearch](https://github.com/elastic/elasticsearch) built upon [Lucene](https://lucene.apache.org/core/)
- [OpenSearch (aka OpenDistro)](https://github.com/opensearch-project/OpenSearch) built upon [Lucene](https://lucene.apache.org/core/)
- [Vespa](https://github.com/vespa-engine/vespa), built with its own search library
- [Solr](https://solr.apache.org/) built upon [Lucene](https://lucene.apache.org/core/)

These engines are all very performant and have many many features, [Elasticsearch](https://github.com/elastic/elasticsearch) is certainly the one that has the bigger community and most features, they are clearly out of reach today for smaller projects like Quickwit which target very specific use cases. Quickwit is not alone in this space and you will found others like [Toshi](https://github.com/toshi-search/Toshi), [Sonic](https://github.com/valeriansaliou/sonic), [MeiliSearch](https://github.com/meilisearch/MeiliSearch), [Typesense](https://github.com/typesense/typesense), [Bleve](https://github.com/blevesearch/bleve), [Bayard](https://github.com/bayard-search/bayard). Among them, a few only are very active, MeiliSearch and Typesense.

Competing in this domain seems not like an easy path and seeing the strength of search engine leaders can discourage many engineers. But that was not our feelink, actually we found it pretty challenging and last year we even spotted an opportunity
to bring something new to this landscape: build a truly decoupled storage and compute search engine. In the analytics domain, this has been a reality for a while (see [Presto](https://github.com/prestodb/presto), [Spark](https://github.com/apache/spark), [Athena](https://aws.amazon.com/athena/)) but not for search engine (see [UltraWarm](https://www.youtube.com/watch?v=RaLBuVZSbh0) and [ES frozen tier](https://www.elastic.co/fr/blog/introducing-elasticsearch-frozen-tier-searchbox-on-s3)).

That's how Quickwit is born and it now becomes a reality with our [first release](../getting-started/installation.md). Not only this brings a new solution more cost-efficient, but our search clusters are also easier to operate. One can add or remove search instances in seconds Multi-tenant search becomes trivial. And that's only the beginnnig, see our [features page](features.md) to get into the details or look at our [roadmap](https://github.com/quickwit-inc/quickwit/projects/1) to learn what will come in the next months.

Last but not least, we don't pretend to compete with the big four but we are convinced that our ascetic quest to truly separate compute and storage will help many engineers.


