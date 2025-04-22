# Doc-Transformers

Replicate the functionality of log processing pipelines of datadog.
See [datadog documentation](https://docs.datadoghq.com/logs/log_configuration/processors/?tab=ui)

# TODO

- Log events can be submitted up to 18 hours in the past and two hours in the future. 
=> Dropping events would need to be done after execting the pipelines, since the pipeline can contain date remapping.

- [ ] check how to handle arrays in remove_nested_from_map
- [ ] check how to handle arrays in get_nested_values

### Filters
* Handle more than numerical ranges in filter.rs (compare fn)
* Do field normalization after JSON parsing
* Replicate percolation on `message`: uncommon default field matching behaviour (see commented test)

### Transformers
- [ ] arithmetic-processor
- [x] attribute-remapper
  - [ ] Missing `overrideOnConflict` 
- [x] category-processor
- [x] date-remapper
- [ ] geo-ip-parser
- [x] grok-parser
  - [x] Add rulenames parsing in grok.rs
- [ ] lookup-processor
- [x] message-remapper
- [x] pipeline
- [x] service-remapper
- [x] status-remapper
- [x] string-builder-processor
- [x] trace-id-remapper
- [ ] url-parser
- [ ] user-agent-parser
