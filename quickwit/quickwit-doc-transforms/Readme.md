# Doc-Transformers

Replicate the functionality of log processing pipelines of datadog.
See [datadog documentation](https://docs.datadoghq.com/logs/log_configuration/processors/?tab=ui)

# TODO

### Filters
* Handle more than numerical ranges in filter.rs (compare fn)
* Do field normalization after JSON parsing
* Replicate percolation on `message`: uncommon default field matching behaviour (see commented test)

### Transformers
- [ ] arithmetic-processor
- [x] attribute-remapper
  - [ ] Missing `overrideOnConflict` 
- [ ] category-processor
- [ ] date-remapper
- [ ] geo-ip-parser
- [x] grok-parser
  - [ ] Add rulenames parsing in grok.rs
- [ ] lookup-processor
- [ ] message-remapper
- [x] pipeline
- [ ] service-remapper
- [x] status-remapper
- [ ] string-builder-processor
- [ ] trace-id-remapper
- [ ] url-parser
- [ ] user-agent-parser
