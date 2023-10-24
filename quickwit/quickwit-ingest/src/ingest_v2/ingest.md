## Replication

### Settings

- ingest request timeout (30s), `Itimeout`
- persist request timeout (6s), `Ptimeout`
- replicate request timeout (3s), `Rtimeout`
- number of persist attempts (5), `k`

Knowing that persist requests issue replicate requests, and ingest requests issue persist requests, we must have approximately:
- `Ptimeout` >= 2 * `Rtimeout`
- `Itimeout` >= `k` * `Ptimeout`
