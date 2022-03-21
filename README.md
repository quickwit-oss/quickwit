[![codecov](https://codecov.io/gh/quickwit-oss/quickwit/branch/main/graph/badge.svg?token=06SRGAV5SS)](https://codecov.io/gh/quickwit-oss/quickwit)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.0-4baaaa.svg)](CODE_OF_CONDUCT.md)
[![License: AGPL V3](https://img.shields.io/badge/license-AGPL%20V3-blue)](LICENCE.md)
![Twitter Follow](https://img.shields.io/twitter/follow/Quickwit_Inc?color=%231DA1F2&logo=Twitter&style=plastic)
![Discord](https://img.shields.io/discord/908281611840282624?logo=Discord&logoColor=%23FFFFFF&style=plastic) 
![Rust](https://img.shields.io/badge/Rust-black?logo=rust&style=plastic)
![CI](https://github.com/quickwit-oss/quickwit/actions/workflows/ci.yml/badge.svg)
![Tantivy](https://shields.io/badge/tantivy-A3B6D2?labelColor=A3B6D2&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAHgAAAB4CAMAAAAOusbgAAAABGdBTUEAALGPC/xhBQAAAAFzUkdCAK7OHOkAAAAJcEhZcwAAB2IAAAdiATh6mdsAAAMAUExURUxpcUxpcc3Z4r7a7kRERExpcVpgZ0ZGRjs3NE9SVUZGRm58i0REREpLTEhISVNXXFNYXUhKSkhISWdxflBUWEhISJmz0U1PUlNXXEpMTXODlE5QU1ZbYmJqdVJWW0hJSkpMTkxPUkpKTJu310hKSnmKm1phaElKSnyNomRueD8/P1thaUxpcWx4hUxPUV9ncmNtd////2VuenOCk19ncJGnwFNYXUxOUF5mb05RVVpgaFBTV0hISlJVW01QUlJWWl5mb1ZbYEhJSkxOUHuLn09RVVtiakxNUFBSVkxOUElKS2Foc1hdZEhJSlJVWkxOUk1RU0xOUUpKTFFVWlJWW01PU09SVl5mb2t3hUtOUEhJSkZISE1QUl5lb1ddY1tiaVFVWVBTV2hzf1lfZ1JVWWlyf1phaUZISEpMToics0xOUEdISUlKTl5lb0pLTlFVW1ZbYUxOUEhKS0dISU1QU1FVWlFVWUZFRUtOUEhISkpMTkxOUExOUEhISlVbYU5QVUpMTk9TV1lfZywnIlNXXF5nb0dISW56iIygt1BSVlthbmx4h0ZGSFRZXGdnZ0pMTEVLUmhyfUpMTk5SVFJWXEpMTm58i0lJSlBTV2x5iEZGRklLTdzg6FZbYElLTFVaYE1QU0pMTVBUV05RVE9SVkZGR3yPoUpMTkpOUlheZEhKSkhKTE5QVE5SVP39/0RGSFheZkpKTEpMUM3NzWJqdkxOUEpMTnR2ekhISE5SVkhISGJsdvPz9VxiakxOUFRYXEJERDg4OFxialxialJYXOfr72BochQUFHx6eouTnZufo/Hz90ZGRExOUEZGSNHf9eft8eXp7ff5+0ZGRkRERP///0BAQEJCQv39/UZGSEZISEhISEVGRUhJSkVERTs7O0NEQj4+Pvf391BQUMnJye3t7fPz8/r6+nZ2dlRUVMPDw1hYWOjo6b+/v4mJiWBgYFpaWkA/PpOTk7u7u9fX17e3t2JiYjAwMHx8fGxsbK2trd3d3dXV1aWlpW5ubufn52hcCAsAAADTdFJOUwAAAgT9AAL9AgL7Gfva+WJT9P0VhvQIt2fNDqhO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)

<br/>
<br/>
<br/>
<p align="center">
  <img src="docs/assets/images/logo_horizontal.svg" alt="Quickwit" height="60">
</p>

<h2 align="center">
Search more with less.
</h2>
<h4 align="center">Manage your logs at any scale, on any budget.
</h4>
<h4 align="center">
  <a href="https://quickwit.io/docs/get-started/quickstart">Quickstart</a> |
  <a href="https://quickwit.io/docs/">Docs</a> |
  <a href="https://quickwit.io/docs/guides/tutorial-hdfs-logs">Tutorials</a> |
  <a href="https://discord.gg/rpRRTezWhW">Chat</a> |
  <a href="https://quickwit.io/docs/get-started/installation">Download</a>
</h4>
<br/>

Quickwit is the next-gen search engine built for logs. It  is a highly reliable & cost-efficient alternative to Elasticsearch. 

❗Disclaimer: Some of the features in this README are describing Quickwit 0.3

<img src="quickwit-ui-wip.png"> 


# 💡 Features



-  Up to 10x cheaper on average compared to Elastic - [learn how.](https://quickwit.io/blog/commoncrawl)
- Index data persisted on object storage
- Ingest & Search API Elasticsearch compatible
- Works out of the box with sensible defaults 
- Indexed JSON documents with or without a strict schema.
- Cloud native: kubernetes ready 
- Add and remove nodes in seconds
- Runs on a fraction of the resources: written in Rust, powered by tantivy.
- Sleep like a log: all your data is safely stored on object storage (AWS S3...)
- Optimized for multi-tenancy. Add and scale tenants with no overhead costs.
- Exactly-once semantics.
- Decoupled compute & storage.





## 🔮 Upcoming Features
-  Ingest ES-compatible API<br>
- Aggregation ES-compatible API<br>
- Schemaless indexing (JSON field)<br>
- Lightweight Embedded UI<br>
- Load directly from Amazon S3<br>

# Uses & Limitations
| ✅ When to use                                                  	| ❌ When not to use                                       	|
|--------------------------------------------------------------	|--------------------------------------------------------------	|
| Your documents are immutable: applications logs, system logs, access logs, user actions logs, audit trail, etc.                    	| Your data is mutable.   	|
| Your data has a time component.                              	| You need a low-latency search for e-commerce websites.               	|
| You want full-text search in a multi-tenant environment.     	| Search relevancy / scoring is a key feature for your search. 	| 
| You want to index from Kafka. | High QPS...
| Clickhouse... 
|                                                              	|
| 	|                                                              	|

# ⚙️ Getting Started


Let's download and install Quickwit.

```markdown
curl -L https://install.quickwit.io | sh
``` 

You can now move this executable directory wherever sensible for your environment and possibly add it to your `PATH` environment. You can also install it via [other means](https://quickwit.io/docs/get-started/installation).

Take a look at our [Quick Start]([https://quickwit.io/docs/get-started/quickstart) to do amazing things, like [Creating your first index](https://quickwit.io/docs/get-started/quickstart#create-your-first-index) or [Adding some documents](https://quickwit.io/docs/get-started/quickstart#lets-add-some-documents), or take a glance at our full [Installation guide](https://quickwit.io/docs/get-started/installation)!


# 📖 Tutorials

- [Search on logs with timestamp pruning](https://quickwit.io/docs/guides/tutorial-hdfs-logs)
- [Setup a distributed search on AWS S3](https://quickwit.io/docs/guides/tutorial-hdfs-logs-distributed-search-aws-s3)
- [Add full-text search to a well-known OLAP database, Clickhouse](https://quickwit.io/docs/guides/add-full-text-search-to-your-olap-db)

# 💬 Community

📝 Blog Posts 
<!-- BLOG-POST-LIST:START -->
<!-- BLOG-POST-LIST:END -->

📺 Youtube Videos 
<!-- YOUTUBE:START -->
<!-- YOUTUBE:END -->

⚡ What have we done recently? 
<!--START_SECTION:activity--> 

Chat with us in [Discord][discord] <br>
Follow us on [Twitter][twitter]


# ❓FAQ
1. How does Quickwit compare to Elasticsearch?  
- Quickwit enables search on object storage and makes it up to 10x cheaper. 
2. What license does Quickwit use? 
- AGPL v3 - you are free to use Quickwit for your project, as long as you don't modify Quickwit. If you do, make the modifications public or purchase a commercial license to us. 
3. How Quickwit makes money / What is our business model? 
-  We have a commercial license for enterprises with support and a voice in our roadmap. 
-  There is no plan to become SaaS in the near future. <br> <br>
(4. Which object storage do you support?)
(- Amazon S3, ...)

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
 


# 🤝 Contribute
Please read through our [Contributor Covenant Code of Conduct](https://github.com/quickwit-oss/quickwit/blob/0add0562f08e4edd46f5c5537e8ef457d42a508e/CODE_OF_CONDUCT.md).

First issues...

We are grateful for all the contributors that would like to or have already joined Quickwit.


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

