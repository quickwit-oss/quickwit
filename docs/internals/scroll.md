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

## Quirk and difficulty.

The scrolled results should be consistent with the state of the original index.
For this reason we need to capture the state of the index at the point of the original request.

The elasticsearch API is needlessly broken as it returns the same scroll_id over and over.
If a network error happens between the client and the server at page N, there is no way for the client to ask the reemission of page N.
Page N+1 will be returned on the next call.


## Implementation

The scroll context contains:
- the detail about the original query (we need to be able to reemit paginated queries)
- the list of split metadata used for the query
- a cached list of partial docs (= not the doc content, just its address and its score) to avoid
- the total number of results, in order to append that information to our response.
searching at every single scroll requests.

We use a simple leaderless KV store to keep the state required to run the scroll API.
We generate a scroll ULID and use it to get a list of the servers with the best affinity according
to rendez vous hashing. We then go through them in order and attempt to put that key on up to 2 servers.
Failures for these PUTs are silent.

For each call to scroll, one of two things can happen:
- the partial docs for the page requested is in the partial doc cache. We just run the fetch_docs phase,
and update the context with the `start_offset`.
- the partial docs for the page request are not in the partial doc cache. We then run a new search query.

We attempt to fetch `SCROLL_BATCH_LEN` in order to fill the partial doc address cache for subsequent calls.

# A strange `scroll_id`.

For more robustness, the scroll id is the concatenation of the
- ULID: used as the address for the search context.
- the start_offset.

The idea here is that if that if the put request failed, we can still return the right results even if we have an obsolete version of the `ScrollContext`.
We indeed take the max of the start_offset supplied in the `scroll_id` and present in the `ScrollContext`.

# Quickwit specific quirks

Our state is pretty volatile. Some scrolls may end up being broken if we were to remove 2 servers within 30mn.

The scroll lifetime does not extend the life of a scroll context.
We do not anything to prevent splits from being GCed and only rely on the grace period to make sure this does not happen.

The ES API does not always updates the  `_scroll_id`. It does not seem to change in the different calls.
A misimplemented client might therefore appear to work correctly on one shard with elasticsearch and not work on quickwit.


Quickwit caches partial hits in batches of 1000 results.
Querying page N currently  requires fetching the results from up to N*1000.

One possible amelioration could be to add a `search_after` information the context. It is possible to workaround the tie breaker
edge case, by only considering `search_after >=` and adjusting offsets accordingly.

For instance, if batch K ends with hit K*1_000, and the last 3 hits have the same score S. We can store S, and the number of docs with a score S to skip: 3.  To then fetch batch `K+1`, we can filter out all pages with a score < S, and fetch hits from 3..1_003.

