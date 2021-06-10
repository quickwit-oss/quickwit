# Quickwit-search

This project implements quickwit's search API.

# Architecture

Quickwit relies on a pool of stateless search servers.
All search-servers are identical and are meant to be queried using a simple load balancer.

The server which receives the query acts as the *root* server for the time of the query.

The *root* role is to coordinate the work of the *leaf* servers:
- it interprets the user query
- queries the meta store to identify the list of relevant index splits
- dispatch the work to the leaf
- gathers and merge the leaf results.

The *leaf* servers are in charge of performing the actual search task on their
assigned subset of index splits.

A search request on one split typically works in phases
- downloading the hotcache and opening the directory
- download all of the data of required for the query phase on the split
- performing the query_search_phase
- if required, performing the fetch_docs_phase.
