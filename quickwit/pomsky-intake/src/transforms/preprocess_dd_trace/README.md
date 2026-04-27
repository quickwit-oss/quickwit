# Spans pipeline migration

Tracks the port of the Java [`SpansProcessingPipeline`][src] global pipeline
(~60 processors) to Pomsky's `preprocess_dd_trace` + `preprocess_span`
transforms.

[src]: https://github.com/DataDog/logs-backend/blob/prod/domains/apm/apps/apm-processing/src/main/java/com/dd/logs/processing/pipelines/SpansProcessingPipeline.java

Each row records the Java processor, the level it operates at in the Java
pipeline, and the migration status. Levels: `payload` (whole DD chunk batch),
`trace` (one trace = group of spans), `span` (one span). In Pomsky every ported
processor runs at the **span** level — payload/trace logic is folded into
`preprocess_dd_trace` (which sees the chunk pre-explode) or pre-computed and
inlined onto each span at canonicalization time.

Status: `[ ]` todo · `[~]` in progress · `[x]` ported · `[-]` skipped (note why)

## PreProcessing

| Status | Java processor | Java level | Notes |
|--------|----------------|-----------:|-------|
| `[ ]` | TraceMetaMetricsPropagator | trace | propagate root meta/metrics down |
| `[ ]` | SpansTagRemapper | span | |
| `[ ]` | SpansAgentPsrDropper | span | |
| `[ ]` | SpansChunkMerger | payload | runs in `preprocess_dd_trace` |
| `[ ]` | SpansIngestionReasonResolver | payload | runs in `preprocess_dd_trace` |
| `[ ]` | SpansDropperProcessor | payload | runs in `preprocess_dd_trace` |
| `[ ]` | ResourceRenamingProcessor | span | needs `ResourceRenamingContextBlob` |

## Enrichment

| Status | Java processor | Java level | Notes |
|--------|----------------|-----------:|-------|
| `[ ]` | SpansFlagProcessor | payload | |
| `[ ]` | SpansPartialVersionProcessor | trace | |
| `[ ]` | SpansUrlGroupingChunksProcessor | payload | wraps `SpansUrlGroupingProcessor` |
| `[ ]` | SpansNormalizer | payload | |
| `[ ]` | SpansToChunkEnricher | trace | folds into canonicalization |
| `[ ]` | ECSTaskArnExtractor | trace | |
| `[ ]` | ServiceVersionProcessor | payload | |
| `[ ]` | SpansPayloadEnvResolver | payload | |
| `[ ]` | SpansServerlessEnricher | payload | |
| `[ ]` | SpansServerlessOtelProcessor | span | |
| `[ ]` | SpansEnvResolver | payload | |
| `[ ]` | SpansUsageCalculator | payload | |
| `[ ]` | SpansUrlDetailsProcessor | span | |
| `[ ]` | SpansQuantizationProcessor | span | |
| `[ ]` | SCIGoPathProcessor | trace | |
| `[ ]` | CIEventsProcessor | span | conditional: `withCIApp` |
| `[ ]` | CIAppSpansTaggerProcessor | span | conditional: `withCIApp` |
| `[ ]` | JobSpansProcessor | span | |
| `[ ]` | ModelLabSpansProcessor | span | |
| `[ ]` | DataSetSpansProcessor | span | |
| `[ ]` | SpansGrpcStatusCodeProcessor | span | |
| `[ ]` | SensitiveDataRemoverProcessor | span | |
| `[ ]` | ClientIPProcessor | span | |
| `[ ]` | UserAgentProcessor | span | |
| `[ ]` | SCICommitSHANormalizer | payload | |
| `[ ]` | SqlSpanProcessor | span | |
| `[ ]` | RumOtelPayloadAnnotator | payload | |
| `[ ]` | AppSecTopLevelSpansEnricherProcessor | trace | needs `AppSecThreatContextBlob` |
| `[ ]` | AppSecBackendWafSpansProcessor | span | needs WAF runtime |
| `[ ]` | AppSecSpansProcessor | span | needs `AppSecThreatContextBlob` |
| `[ ]` | AppSecApiProcessor | span | needs `AppSecThreatContextBlob` |
| `[ ]` | SpansOTelProcessor | trace | |
| `[ ]` | SpansExternalProviderProcessor | span | |
| `[ ]` | ApimEndpointProcessor | span | conditional: `withAPIManagementApp`; needs `ApimEndpointContextBlob` |
| `[ ]` | SpanLinksProcessor | span | |
| `[ ]` | SpanEventsProcessor | span | |
| `[ ]` | RequestIdRemapper | span | |
| `[ ]` | TraceSignatureProcessor | trace | trace-wide aggregation |
| `[ ]` | TraceHeuristicProcessor | trace | trace-wide aggregation |
| `[ ]` | TraceEmbeddingProcessor | trace | trace-wide aggregation; ML pipeline |

## PostProcessing

| Status | Java processor | Java level | Notes |
|--------|----------------|-----------:|-------|
| `[ ]` | SpansDataReporterProcessor | payload | reporter, not enrichment |
| `[ ]` | SpansHistogramReporterProcessor | payload | reporter |
| `[ ]` | RumSpansMetricsProcessor | span | |
| `[ ]` | AgentSamplersConfigReporter | payload | reporter |
| `[ ]` | SpansOTelUsageReporter | payload | reporter |
| `[ ]` | SpansImmutableFieldFinderProcessor | span | |
| `[ ]` | AppsecSpansMetricsProcessor | trace | reporter |
| `[ ]` | ServiceNameContainerTagsProcessor | trace | reporter |

## Post-Custom (`SpansPostCustomPipeline`)

Runs after the user-defined custom pipeline in the Java service.

| Status | Java processor | Java level | Notes |
|--------|----------------|-----------:|-------|
| `[ ]` | UsagePointsProcessor | payload | |
| `[ ]` | EmitSamplingDecisionsProcessor | payload | |
| `[ ]` | EC2CCRIDProcessor | payload | |

## Other pipelines (likely out of scope)

The Java service composes additional pipelines around the global one. None are
planned for the initial Pomsky port; tracked here for visibility.

- **Error Tracking** — `ErrorTrackingChunksProcessor` (rules + rate limiter + rewriter). Needs `ErrorTrackingContext`.
- **Threat Intel** — `IPThreatIntelIndicator`, `FlaggedIPsThreatIntelIndicator`, `EmailDomainThreatIntelIndicator`, `UserAgentThreatIntelIndicatorSpans`, `AppSecThreatIntelProcessor`. Needs threat-intel tables.
- **Sampling** — `SpansServiceResolutionProcessor`, `SpansServiceTagEnrichmentProcessor`, `SpansSamplingProcessor` (one per user-defined sampling pipeline), CI test/pipeline keep-all processors, default `errorsProcessor`. Needs Mongo-driven sampling configs.
- **Trace Query Library** — `TraceQueryLibraryProcessor` × 2 (internal configurable extraction + Trace2Metrics). Needs `Trace2MetricsSubContext`.
- **Sensitive Data Scanner** — `SpansSensitiveDataScanner`. Needs `SdsContext`.
- **AppSec Spans Exclusion** — `AppSecSpansExclusionProcessor`. Needs Mongo-driven exclusion configs.

## Notes on the architecture

- DD agent payload-level and trace-level logic runs in `preprocess_dd_trace` (operates on the whole chunk before explode), or is pre-computed during canonicalization and inlined onto each span as `trace.*` / `payload.*` fields.
- OTLP traces arrive already exploded (Vector source flattens `ResourceSpans → ScopeSpans → Spans`), so trace-wide aggregation is best-effort within a batch. Trace-level processors that require all spans of a trace (e.g. `TraceSignatureProcessor`) cannot run on the OTLP path.
- Span-level processors run in `preprocess_span` and operate on the canonical span shape regardless of source.
