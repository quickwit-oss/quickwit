# Scroll API

The scroll API has been implemented to offer compatibility with ElasticSearch.
The API and the implementation are quirky and are detailed in this document.

## API description

You can find information about the scroll API here.
https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html#scroll-search-results
https://www.elastic.co/guide/en/elasticsearch/reference/current/scroll-api.html

The user runs a regular search request with a `scroll` param.
The search result then contains the normal response, but a `_scroll` property is added to the search body.

That id is then meant to be sent to a scroll rest API.
This API successive calls will then return pages incrementally.
Scroll is limited to 10k items.

## Quirk and difficulty.

The scrolled results should be consistent with the state of the original index.
For this reason we need to capture the state of the index at the point of the original request.

If a network error happens between the client and the server at page N, there is no way for the client to ask the reemission of page N.
Page N+1 will be returned on the next call.

## Implementation

Server side, we store a replicated scroll context.

It contains:
- the detail about the original query (we need to be able to reemit paginated queries)
- the "point-in-time" list of split metadatas used for the query
- a cached list of partial docs (= not the doc content, just its address and its score) to avoid
performing search over and over.
- the total number of results, in order to append that information to our response.
searching at every single scroll requests.

We use a simple leaderless KV store to keep the state required to run the scroll API.
We generate a scroll ULID and use it to get a list of the servers with the best affinity according
to rendez vous hashing. We then go through them in order and attempt to put that key on up to 2 servers. Failures for these PUTs are silent.

For each call to scroll, one of two things can happen:
- the partial docs for the page requested is in the partial doc cache. We just run the fetch_docs phase, and update the context with the `start_offset`.
- the partial docs for the page request are not in the partial doc cache. We then run a new search query.

We attempt to fetch `SCROLL_BATCH_LEN` in order to fill the partial doc address cache for subsequent calls.

# A strange `scroll_id`.

The elasticsearch API is needlessly broken as it returns the same scroll_id most of the time.
The "page-change" mutation is something that happens on the server side.

In quickwit on the other hand, the scroll id is the concatenation of the
- ULID: used as the address for the search context.
- the start_offset.

We only mutate the state server side to update the cache whenever needed.

The idea here is that if that if the put request failed, we can still return the right results even if we have an obsolete version of the `ScrollContext`.
We indeed take the max of the start_offset supplied in the `scroll_id` and present in the `ScrollContext`.

# Quickwit implementation (improvment, quirks and shortcuts)

We do not do explicitly protect the split from our store Point-In-Time information
from deletion. Instead we simply rely on the existing grace period mechanism (a split
only is effectively garbage collected 32mn after it is marked as deleted).

For this reason we limit the scroll period to 30mn and subsequent scroll calls do not
extend the scroll period.

Also thanks to this period, we do not add any extra replication repair mechanism.
Some scroll calls will end up being broken if we were to remove 2 servers within 30mn.

Quickwit caches partial hits in batches of 1000 results.
Querying page N currently  requires fetching the results from up to N*1000.

One possible amelioration could be to add a `search_after` information the context. It is possible to workaround the tie breaker
edge case, by only considering `search_after >=` and adjusting offsets accordingly.

For instance, if batch K ends with hit K*1_000, and the last 3 hits have the same score S. We can store S, and the number of docs with a score S to skip: 3.  To then fetch batch `K+1`, we can filter out all pages with a score < S, and fetch hits from 3..1_003.
