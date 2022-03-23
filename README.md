![CI](https://github.com/quickwit-oss/quickwit/actions/workflows/ci.yml/badge.svg)
[![codecov](https://codecov.io/gh/quickwit-oss/quickwit/branch/main/graph/badge.svg?token=06SRGAV5SS)](https://codecov.io/gh/quickwit-oss/quickwit)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.0-4baaaa.svg)](CODE_OF_CONDUCT.md)
[![License: AGPL V3](https://img.shields.io/badge/license-AGPL%20V3-blue)](LICENCE.md)
![Twitter Follow](https://img.shields.io/twitter/follow/Quickwit_Inc?color=%231DA1F2&logo=Twitter&style=plastic)
![Discord](https://img.shields.io/discord/908281611840282624?logo=Discord&logoColor=%23FFFFFF&style=plastic) 
![Rust](https://img.shields.io/badge/Rust-black?logo=rust&style=plastic)
<br/>

<br/>
<br/>
<p align="center">
  <img src="docs/assets/images/logo_horizontal.svg" alt="Quickwit" height="60">
</p>

<h3 align="center">
Search more with less
</h3>
  
<h4 align="center">The new way to manage your logs at any scale
</h4>
<h4 align="center">
  <a href="https://quickwit.io/docs/get-started/quickstart">Quickstart</a> |
  <a href="https://quickwit.io/docs/">Docs</a> |
  <a href="https://quickwit.io/docs/guides/tutorial-hdfs-logs">Tutorials</a> |
  <a href="https://discord.gg/rpRRTezWhW">Chat</a> |
  <a href="https://quickwit.io/docs/get-started/installation">Download</a>
</h4>
<br/>

❗**Disclaimer: you are reading the README of Quickwit 0.3 version that will be shipped by the end of April 2022.**

Quickwit is the next-gen search & analytics engine built for logs. It is a highly reliable & cost-efficient alternative to Elasticsearch.

<br/>

<img src="docs/assets/images/quickwit-ui-wip.png"> 

<br/>

# 💡 Features

- Up to 10x cheaper on average compared to Elastic - [learn how](https://quickwit.io/blog/commoncrawl)
- Index data persisted on object storage
- Indexed JSON documents with or without a strict schema (JSON Field)
- Ingest, Search & Aggregation API Elasticsearch compatible
- Lightweight Embedded UI
- Cloud-native: Kubernetes ready
- Add and remove nodes in seconds
- Runs on a fraction of the resources: written in Rust, powered by the mighty tantivy
- Sleep like a log: all your data is safely stored on object storage (AWS S3...)
- Optimized for multi-tenancy. Add and scale tenants with no overhead costs
- Exactly-once semantics
- Kafka-native ingestion
- Search stream API that notably unlocks full-text search in ClickHouse
- Decoupled compute & storage
- Works out of the box with sensible defaults


### 🔮 Upcoming Features
- Ingest your logs from your object storage
- Distributed indexing
- Support for tracing
- Native support for OpenTelemetry

# Uses & Limitations
| ✅ When to use                                                  	| ❌ When not to use                                       	|
|--------------------------------------------------------------	|--------------------------------------------------------------	|
| Your documents are immutable: application logs, system logs, access logs, user actions logs, audit trail, etc.                    	| Your documents are mutable.   	|
| Your data has a time component. Quickwit includes optimizations and design choices specifically related to time. | You need a low-latency search for e-commerce websites.               	|
| You want a full-text search in a multi-tenant environment.     	| You provide a public-facing search with high QPS.	| 
| You want to index directly from Kafka. | Search relevancy and scoring is a key feature for your search.
| You want to add a full-text search to your ClickHouse cluster.
| You ingest a tremendous amount of logs and don't want to pay huge bills.                                                             	|
| You ingest a tremendous amount of data and you don't want to have to waste your time babysitting your cluster.

# ⚡  Getting Started


Let's download and install Quickwit.

```markdown
curl -L https://install.quickwit.io | sh
``` 

You can now move this executable directory wherever sensible for your environment and possibly add it to your `PATH` environment. You can also install it via [other means](https://quickwit.io/docs/get-started/installation).

Take a look at our [Quick Start]([https://quickwit.io/docs/get-started/quickstart) to do amazing things, like [Creating your first index](https://quickwit.io/docs/get-started/quickstart#create-your-first-index) or [Adding some documents](https://quickwit.io/docs/get-started/quickstart#lets-add-some-documents), or take a glance at our full [Installation guide](https://quickwit.io/docs/get-started/installation)!


# 📚 Tutorials

- [Search on logs with timestamp pruning](https://quickwit.io/docs/guides/tutorial-hdfs-logs)
- [Setup a distributed search on AWS S3](https://quickwit.io/docs/guides/tutorial-hdfs-logs-distributed-search-aws-s3)
- [Add full-text search to a well-known OLAP database, Clickhouse](https://quickwit.io/docs/guides/add-full-text-search-to-your-olap-db)

# 💬 Community

- Chat with us in [Discord][discord]
- 📝 [Blog Posts](https://quickwit.io/blog)
- 📺 [Youtube Videos](https://www.youtube.com/channel/UCvZVuRm2FiDq1_ul0mY85wA)
- Follow us on [Twitter][twitter]


# 🙋 FAQ
###  How Quickwit is different from traditional search engines like Elasticsearch or Solr?
The core difference and advantage of Quickwit is its architecture built from the ground-up for the cloud. We decoupled compute and storage for search, designed highly-performant index data structures, optimized IO paths to make it fast on object storage, and implemented highly-reliable distributed search so all engineers can sleep at night. Last but not least, we put our love in every line of code.
### What license does Quickwit use? 
Quickwit is open-source under the GNU Affero General Public License Version 3 - AGPLv3. Fundamentally, this means that you are free to use Quickwit for your project, as long as you don't modify Quickwit. If you do, you have to make the modifications public.
We also provide a commercial license for enterprises to provide support and a voice on our roadmap.

### What is Quickwit business model?
Our business model relies on our commercial license. There is no plan to become SaaS in the near future.


# 🪄 Third Party Integration
<p align="left">
<img align="center" src="https://kafka.apache.org/logos/kafka_logo--simple.png" alt="quickwit_inc" height="30" width="auto" /> &nbsp;
<img align="center" src="https://www.postgresql.org/media/img/about/press/elephant.png" alt="quickwit_inc" height="30" width="auto"/> &nbsp;&nbsp;&nbsp;
<img align="center" src="https://upload.wikimedia.org/wikipedia/commons/thumb/9/93/Amazon_Web_Services_Logo.svg/1200px-Amazon_Web_Services_Logo.svg.png" alt="quickwit_inc" height="25" width="auto" /> &nbsp; &nbsp;
<img align="center" src="https://www.altisconsulting.com/au/wp-content/uploads/sites/4/2018/05/AWS-Kinesis.jpg" alt="quickwit_inc" height="30" width="auto"/> &nbsp;
<img align="center" src="https://min.io/resources/img/logo.svg" alt="quickwit_inc" height="10" width="auto"/> &nbsp;&nbsp;
<img align="center" src="https://humancoders-formations.s3.amazonaws.com/uploads/course/logo/180/formation-kubernetes.png" alt="quickwit_inc" height="30" width="auto"/> &nbsp; 
<img align="center" src="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQVNN6B4PQImnG_h6-chmWm4BC7Jm-M31O0-eUW2Ta3elfdyj5udQytjbG_99mNwUl-2tM&usqp=CAU" height="30" width="auto"/>
</p>
 

# 🤝 Contribute and spread the word

We are always super happy to have contributions: code, documentation, issues, feedback, or even saying hello on discord! Here is how you can get started: 
- Have a look through GitHub issues labeled "Good first issue".
- Read our [Contributor Covenant Code of Conduct](https://github.com/quickwit-oss/quickwit/blob/0add0562f08e4edd46f5c5537e8ef457d42a508e/CODE_OF_CONDUCT.md)
- Create a fork of Quickwit and submit your pull request!

✨ And to thank you for your contributions, claim your swag by emailing us at hello at quickwit.io.


# 🔗 Reference
- [Quickwit CLI](https://quickwit.io/docs/reference/cli)
- [Index Config](https://quickwit.io/docs/reference/index-config)
- [Search API](https://quickwit.io/docs/reference/rest-api)
- [Query language](https://quickwit.io/docs/reference/query-language)
- [Code of conduct](CODE_OF_CONDUCT.md)
- [Contributing](CONTRIBUTING.md)



[website]: https://quickwit.io/
[youtube]: https://www.youtube.com/channel/UCvZVuRm2FiDq1_ul0mY85wA
[twitter]: https://twitter.com/Quickwit_Inc
[discord]: https://discord.gg/MT27AG5EVE
[blogs]: https://quickwit.io/blog

