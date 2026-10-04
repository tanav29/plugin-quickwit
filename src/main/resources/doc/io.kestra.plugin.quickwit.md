# Quickwit

[Quickwit](https://quickwit.io) is a cloud-native search engine for logs, traces and other immutable data. This plugin
exposes its REST API as typed Kestra tasks, so a flow can search indexes, ingest documents and manage indexes, sources and
delete tasks without hand-written HTTP calls.

## What it ships

| Package | Tasks |
|---|---|
| `search` | `Search`, `Trigger` |
| `ingest` | `Ingest` |
| `index` | `Create`, `Get`, `List`, `Delete`, `Clear` |
| `source` | `Create`, `Toggle`, `Delete`, `ResetCheckpoint` |
| `deletetask` | `Create`, `List` |

## Connecting

Every task and the trigger take the same connection properties, declared flat on the task:

| Property | Description |
|---|---|
| `url` | Base URL of the REST API, scheme included. The `api/v1` prefix is added for you. |
| `basicAuth` | Optional HTTP basic credentials, for clusters behind a reverse proxy. |
| `headers` | Optional headers sent on every request, for example a gateway bearer token. |
| `connectTimeout` / `readTimeout` | ISO-8601 durations, e.g. `PT10S`. |

A local node listens on **port 7280**, so the default self-hosted URL is `http://localhost:7280`.

### Authentication

Quickwit itself has **no authentication layer**: its REST server ships CORS, compression and tracing only, and its OpenAPI
document declares no security scheme. Authentication is something you add in front of the cluster. That is why `basicAuth`
and `headers` are both optional:

- leave them unset when the REST API is reachable directly, on a trusted network;
- set `basicAuth` when a reverse proxy (nginx, Envoy) enforces HTTP basic auth;
- set `headers` when an API gateway injects credentials, for example `Authorization: Bearer ...` or `X-API-Key`.

Keep credentials in secrets, never in the flow itself:

```yaml
headers:
  Authorization: "Bearer {{ secret('QUICKWIT_GATEWAY_TOKEN') }}"
```

## Searching

```yaml
id: quickwit_search
namespace: company.team

tasks:
  - id: search
    type: io.kestra.plugin.quickwit.search.Search
    url: "https://quickwit.example.com:7280"
    index: app-logs
    query: "severity:ERROR"
    startTimestamp: "{{ now() | dateAdd(-1, 'HOURS') | timestamp }}"
    maxHits: 100
    fetchType: FETCH

  - id: log_count
    type: io.kestra.plugin.core.log.Log
    message: "Found {{ outputs.search.size }} errors"
```

`index` also accepts a [multi-target expression](https://quickwit.io/docs/reference/rest-api#multi-target-syntax) such as
`app-logs-000001,app-logs-000002` or a wildcard such as `app-logs*`.

Only the current result page comes back, so set `maxHits` when you need more. Use `fetchType` to choose between `FETCH`
(every hit), `FETCH_ONE` (the first hit), `STORE` (hits written to Kestra internal storage, returned as a URI) and `NONE`.

## Reacting to new documents

`search.Trigger` polls the index on an interval. It sorts ascending on `timestampField` and stores the timestamp up to which
documents have been delivered in the **namespace KV Store**. That watermark advances only after the execution is created, so a
document is delivered once. Documents that arrive late, with an event timestamp already below the watermark, are not picked up.

```yaml
id: quickwit_error_watch
namespace: company.team

triggers:
  - id: on_errors
    type: io.kestra.plugin.quickwit.search.Trigger
    url: "https://quickwit.example.com:7280"
    index: app-logs
    query: "severity:ERROR"
    timestampField: timestamp
    interval: PT5M

tasks:
  - id: notify
    type: io.kestra.plugin.core.log.Log
    message: "New errors: {{ trigger.documents | length }}"
```

The first poll returns everything currently matching, which is usually what you want for a first run. Override `stateKey`
only if you deliberately want two triggers to share a watermark.

## Ingesting

Quickwit only accepts NDJSON on the ingest API, so this task serializes whatever it is given. `from` accepts a single
document, a list of documents, a `kestra://` internal storage URI, a local `file://` path, or a JSON string.

```yaml
id: quickwit_ingest
namespace: company.team

inputs:
  - id: file
    type: FILE

tasks:
  - id: ingest
    type: io.kestra.plugin.quickwit.ingest.Ingest
    url: "https://quickwit.example.com:7280"
    index: app-logs
    from: "{{ inputs.file }}"
    commit: WAIT_FOR
```

Two things worth knowing:

- **Visibility.** With the default `commit: AUTO`, documents are queued but not searchable yet. Use `WAIT_FOR` to wait for
  the next split commit, or `FORCE` to commit immediately and wait. `FORCE` is what you want when a search runs right after
  the ingest; it costs performance on small batches.
- **Batch size.** The payload is capped at 10 MB by default. Send a dataset larger than that across several runs, or use a
  Quickwit source instead.

Set `detailedResponse: true` when documents are rejected and you need to know why: Quickwit then returns a `parseFailures`
entry per rejected document, which usually points at a doc mapping mismatch.

## Managing indexes

```yaml
tasks:
  - id: create
    type: io.kestra.plugin.quickwit.index.Create
    url: "http://localhost:7280"
    index: app-logs
    configVersion: "0.8"
    docMapping:
      timestamp_field: timestamp
      field_mappings:
        - name: timestamp
          type: datetime
          input_formats: [unix_timestamp]
          fast: true
        - name: message
          type: text
    retention:
      period: "30 days"
      schedule: "@daily"
```

`configVersion` is named that way because `version` is reserved by Kestra to pin a plugin version. It must match the version
of your Quickwit cluster.

`Clear` drops every split and resets the source checkpoints while keeping the definition; `Delete` removes the index and its
split files for good.

Note that Quickwit also accepts an index configuration written as YAML, but this task sends JSON, which cannot carry YAML
comments or anchors.

## Managing sources

`source.Create` attaches a Kafka, Kinesis or Pulsar source to an index. `source.Toggle` pauses and resumes indexing without
losing the checkpoint, which is the right tool for a maintenance window. `source.ResetCheckpoint` replays a source from the
beginning, for example after a doc mapping change.

## Deleting documents

`deletetask.Create` queues a delete task that removes every document matching a query. The call returns as soon as the task is
queued; the cluster's janitor applies it asynchronously.

```yaml
- id: delete
  type: io.kestra.plugin.quickwit.deletetask.Create
  url: "http://localhost:7280"
  index: app-logs
  query: "*"
  startTimestamp: "{{ now() | dateAdd(-30, 'DAYS') | timestamp }}"
  endTimestamp: "{{ now() | dateAdd(-29, 'DAYS') | timestamp }}"
```

## Errors

Failed requests surface Quickwit's own `message` field, so a doc mapping mismatch or a missing index reads as a Quickwit
error rather than an opaque HTTP status:

```
Quickwit ingest into index 'app-logs' failed with HTTP 400: doc mapping does not contain field 'timestamp'
```

## Requirements

Quickwit serves each endpoint from a specific service, so a single node may not answer all of them:

- the **ingest** API needs a node running an indexer;
- the **search** API needs a node running a searcher.

Point `url` at a node that serves what you call, or at a load balancer in front of the cluster.

## Running a local cluster

```bash
# the image entrypoint is a bare `quickwit`, so the `run` subcommand is required
docker run -p 7280:7280 quickwit/quickwit:latest run
```

Then verify the API is up before running a flow:

```bash
curl http://localhost:7280/api/v1/cluster
```

## Documentation

- [Quickwit documentation](https://quickwit.io/docs)
- [Quickwit REST API reference](https://quickwit.io/docs/reference/rest-api)
- [Quickwit query language](https://quickwit.io/docs/reference/query-language)