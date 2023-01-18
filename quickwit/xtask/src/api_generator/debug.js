Api { 
    common_params: {
        "error_trace": Type { 
            ty: Boolean, description: Some("Include the stack trace of returned errors."), 
            options: [], 
            default: Some(Bool(false)) 
        }, 
        "filter_path": Type { ty: List, description: Some("A comma-separated list of filters used to reduce the response."), 
            options: [], 
            default: None 
        }, 
        "human": Type { 
            ty: Boolean, 
            description: Some("Return human readable values for statistics."), 
            options: [], 
            default: Some(Bool(true)) 
        }, 
        "pretty": Type { 
            ty: Boolean, 
            description: Some("Pretty format the returned JSON response."), 
            options: [], 
            default: Some(Bool(false)) 
        }, 
        "source": Type { 
            ty: String, 
            description: Some("The URL-encoded request definition. Useful for libraries that do not accept a request body for non-POST requests."), 
            options: [], 
            default: None 
        }
    }, 
    root: ApiNamespace { 
        stability: Stable, 
        endpoints: {
            "search": ApiEndpoint { 
                full_name: Some("search"), 
                documentation: Documentation { url: Some(DocumentationUrlString("https://www.elastic.co/guide/en/elasticsearch/reference/0.4/search-search.html")), 
                description: Some("Returns results matching a query.") 
            }, 
            stability: Stable, 
            url: Url { 
                paths: [
                    Path { path: PathString("/_search"), methods: [Get, Post], parts: {}, deprecated: None }, 
                    Path { path: PathString("/{index}/_search"), methods: [Get, Post], parts: {"index": Type { ty: List, description: Some("A comma-separated list of index names to search; use `_all` or empty string to perform the operation on all indices"), options: [], default: None }}, 
                    deprecated: None }
                ] 
            }, 
            deprecated: None, 
            params: {
                "_source": Type { ty: List, description: Some("True or false to return the _source field or not, or a list of fields to return"), options: [], default: None }, 
                "_source_excludes": Type { ty: List, description: Some("A list of fields to exclude from the returned _source field"), options: [], default: None }, 
                "_source_includes": Type { ty: List, description: Some("A list of fields to extract and return from the _source field"), options: [], default: None }, 
                "allow_no_indices": Type { ty: Boolean, description: Some("Whether to ignore if a wildcard indices expression resolves into no concrete indices. (This includes `_all` string or when no indices have been specified)"), options: [], default: None }, 
                "allow_partial_search_results": Type { ty: Boolean, description: Some("Indicate if an error should be returned if there is a partial search failure or timeout"), options: [], default: Some(Bool(true)) }, 
                "analyze_wildcard": Type { ty: Boolean, description: Some("Specify whether wildcard and prefix queries should be analyzed (default: false)"), options: [], default: None }, 
                "analyzer": Type { ty: String, description: Some("The analyzer to use for the query string"), options: [], default: None }, 
                "batched_reduce_size": Type { ty: Number, description: Some("The number of shard results that should be reduced at once on the coordinating node. This value should be used as a protection mechanism to reduce the memory overhead per search request if the potential number of shards in the request can be large."), options: [], default: Some(Number(512)) }, 
                "ccs_minimize_roundtrips": Type { ty: Boolean, description: Some("Indicates whether network round-trips should be minimized as part of cross-cluster search requests execution"), options: [], default: Some(String("true")) }, 
                "default_operator": Type { ty: Enum, description: Some("The default operator for query string query (AND or OR)"), options: [String("AND"), String("OR")], default: Some(String("OR")) }, "df": Type { ty: String, description: Some("The field to use as default where no field prefix is given in the query string"), options: [], default: None }, "docvalue_fields": Type { ty: List, description: Some("A comma-separated list of fields to return as the docvalue representation of a field for each hit"), options: [], default: None }, 
                "expand_wildcards": Type { ty: Enum, description: Some("Whether to expand wildcard expression to concrete indices that are open, closed or both."), options: [String("open"), String("closed"), String("hidden"), String("none"), String("all")], default: Some(String("open")) }, 
                "explain": Type { ty: Boolean, description: Some("Specify whether to return detailed information about score computation as part of a hit"), options: [], default: None }, 
                "force_synthetic_source": Type { ty: Boolean, description: Some("Should this request force synthetic _source? Use this to test if the mapping supports synthetic _source and to get a sense of the worst case performance. Fetches with this enabled will be slower the enabling synthetic source natively in the index."), options: [], default: None }, 
                "from": Type { ty: Number, description: Some("Starting offset (default: 0)"), options: [], default: None }, 
                "ignore_throttled": Type { ty: Boolean, description: Some("Whether specified concrete, expanded or aliased indices should be ignored when throttled"), options: [], default: None }, 
                "ignore_unavailable": Type { ty: Boolean, description: Some("Whether specified concrete indices should be ignored when unavailable (missing or closed)"), options: [], default: None }, 
                "lenient": Type { ty: Boolean, description: Some("Specify whether format-based query failures (such as providing text to a numeric field) should be ignored"), options: [], default: None }, "max_concurrent_shard_requests": Type { ty: Number, description: Some("The number of concurrent shard requests per node this search executes concurrently. This value should be used to limit the impact of the search on the cluster in order to limit the number of concurrent shard requests"), options: [], default: Some(Number(5)) }, 
                "min_compatible_shard_node": Type { ty: String, description: Some("The minimum compatible version that all shards involved in search should have for this request to be successful"), options: [], default: None }, 
                "pre_filter_shard_size": Type { ty: Number, description: Some("A threshold that enforces a pre-filter roundtrip to prefilter search shards based on query rewriting if the\u{a0}number of shards the search request expands to exceeds the threshold. This filter roundtrip can limit the number of shards significantly if for instance a shard can not match any documents based on its rewrite method ie. if date filters are mandatory to match but the shard bounds and the query are disjoint."), options: [], default: None }, 
                "preference": Type { ty: String, description: Some("Specify the node or shard the operation should be performed on (default: random)"), options: [], default: None }, 
                "q": Type { ty: String, description: Some("Query in the Lucene query string syntax"), options: [], default: None }, 
                "request_cache": Type { ty: Boolean, description: Some("Specify if request cache should be used for this request or not, defaults to index level setting"), options: [], default: None }, 
                "rest_total_hits_as_int": Type { ty: Boolean, description: Some("Indicates whether hits.total should be rendered as an integer or an object in the rest search response"), options: [], default: Some(Bool(false)) }, "routing": Type { ty: List, description: Some("A comma-separated list of specific routing values"), options: [], default: None }, 
                "scroll": Type { ty: Time, description: Some("Specify how long a consistent view of the index should be maintained for scrolled search"), options: [], default: None }, 
                "search_type": Type { ty: Enum, description: Some("Search operation type"), options: [String("query_then_fetch"), String("dfs_query_then_fetch")], default: None }, 
                "seq_no_primary_term": Type { ty: Boolean, description: Some("Specify whether to return sequence number and primary term of the last modification of each hit"), options: [], default: None }, "size": Type { ty: Number, description: Some("Number of hits to return (default: 10)"), options: [], default: None }, 
                "sort": Type { ty: List, description: Some("A comma-separated list of <field>:<direction> pairs"), options: [], default: None }, 
                "stats": Type { ty: List, description: Some("Specific 'tag' of the request for logging and statistical purposes"), options: [], default: None }, 
                "stored_fields": Type { ty: List, description: Some("A comma-separated list of stored fields to return as part of a hit"), options: [], default: None }, 
                "suggest_field": Type { ty: String, description: Some("Specify which field to use for suggestions"), options: [], default: None }, 
                "suggest_mode": Type { ty: Enum, description: Some("Specify suggest mode"), options: [String("missing"), String("popular"), String("always")], default: Some(String("missing")) }, 
                "suggest_size": Type { ty: Number, description: Some("How many suggestions to return in response"), options: [], default: None }, 
                "suggest_text": Type { ty: String, description: Some("The source text for which the suggestions should be returned"), options: [], default: None }, 
                "terminate_after": Type { ty: Number, description: Some("The maximum number of documents to collect for each shard, upon reaching which the query execution will terminate early."), options: [], default: None }, 
                "timeout": Type { ty: Time, description: Some("Explicit operation timeout"), options: [], default: None }, "track_scores": Type { ty: Boolean, description: Some("Whether to calculate and return scores even if they are not used for sorting"), options: [], default: None }, 
                "track_total_hits": Type { ty: Union((Boolean, Long)), description: Some("Indicate if the number of documents that match the query should be tracked. A number can also be specified, to accurately track the total hit count up to the number."), options: [], default: None }, 
                "typed_keys": Type { ty: Boolean, description: Some("Specify whether aggregation and suggester names should be prefixed by their respective types in the response"), options: [], default: None }, 
                "version": Type { ty: Boolean, description: Some("Specify whether to return document version as part of a hit"), options: [], default: None }
            }, 
            body: Some(Body { description: Some("The search definition using the Query DSL"), required: None, serialize: None }) }} }, 
            namespaces: {}, 
            enums: [
                ApiEnum { name: "default_operator", description: Some("The default operator for query string query (AND or OR)"), values: ["AND", "OR"], stability: Stable }, 
                ApiEnum { name: "expand_wildcards", description: Some("Whether to expand wildcard expression to concrete indices that are open, closed or both."), values: ["open", "closed", "hidden", "none", "all"], stability: Stable }, 
                ApiEnum { name: "search_type", description: Some("Search operation type"), values: ["query_then_fetch", "dfs_query_then_fetch"], stability: Stable }, 
                ApiEnum { name: "suggest_mode", description: Some("Specify suggest mode"), values: ["missing", "popular", "always"], stability: Stable }
            ] 
        }
