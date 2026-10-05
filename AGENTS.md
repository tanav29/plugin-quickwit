# Kestra Quickwit Plugin

## What

- Plugin components under `io.kestra.plugin.quickwit`, wrapping the [Quickwit](https://quickwit.io) REST API.
- Tasks and one polling trigger, grouped in five subpackages: `search`, `ingest`, `index`, `source`, `deletetask`.
- Shared connection properties, response models and HTTP plumbing live in the root package.

## Why

- **What user problem does this solve?** Driving Quickwit from Kestra otherwise means `io.kestra.plugin.core.http.Request` plus hand-built JSON, so every flow repeats the base URL, the `api/v1` prefix, query encoding and error handling.
- **Why would a team adopt this plugin in a workflow?** Search, ingest, index, source and delete-task operations become typed tasks with outputs downstream tasks read directly.
- **What operational/business outcome does it enable?** Log and trace pipelines that load Quickwit and alert on it stay readable, and a doc-mapping mismatch surfaces as Quickwit's own error message rather than an opaque HTTP status.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `quickwit` — `AbstractQuickwitTask` (shared connection properties and request-body helpers),
  `QuickwitService` (endpoint building, request creation, response and error handling),
  `models` (API response POJOs)
- `quickwit.search` — `Search`, `Trigger`
- `quickwit.ingest` — `Ingest`, `Commit`
- `quickwit.index` — `Create`, `Get`, `List`, `Delete`, `Clear`, `AbstractQuickwitIndex`
- `quickwit.source` — `Create`, `Toggle`, `Delete`, `ResetCheckpoint`, `AbstractQuickwitSource`
- `quickwit.deletetask` — `Create`, `List`

A base class only exists when it has more than one subclass: a trigger extends `AbstractTrigger`, not
`Task`, so `search` and `deletetask` keep their properties on the concrete components instead of
declaring an abstract class a single task would extend.

No plugin class lives in the root package: `lintPluginDocs` rejects it (PKG-003). Infrastructure
dependencies (Docker Compose services):

- `app` (Kestra, with the built plugin mounted)
- `quickwit` (a single-node Quickwit cluster on port 7280, for local verification)

### HTTP

All calls go through `io.kestra.core.http.client.HttpClient`; no other HTTP client is used. Two
consequences worth knowing before changing `QuickwitService`:

- `allowFailed` is enabled so the status and raw body are inspected here. `HttpException` sanitizes
  the body it embeds in its message, so a message parsed out of the exception would be corrupted.
  Failures are raised as `IllegalStateException` naming the operation, the status and Quickwit's
  `message` field.
- `pathSegment` percent-encodes. `URLEncoder` alone emits `+` for a space, which is a literal plus
  inside a URI path.

### Key plugin classes

- `io.kestra.plugin.quickwit.search.Search` — queries an index, returns hits as `FETCH`, `FETCH_ONE`, `STORE` or `NONE`
- `io.kestra.plugin.quickwit.search.Trigger` — polling trigger, watermark in the namespace KV Store
- `io.kestra.plugin.quickwit.ingest.Ingest` — sends documents as NDJSON, with `AUTO`/`WAIT_FOR`/`FORCE` commit
- `io.kestra.plugin.quickwit.index.Create` — creates an index from a doc mapping
- `io.kestra.plugin.quickwit.index.Clear` — drops splits and resets checkpoints, keeping the definition
- `io.kestra.plugin.quickwit.source.Toggle` — pauses and resumes indexing without losing the checkpoint
- `io.kestra.plugin.quickwit.deletetask.Create` — queues a delete-by-query task

### Authentication

Quickwit ships no authentication layer: its REST server registers CORS, compression and tracing only,
and its OpenAPI document declares no security scheme. `basicAuth` and `headers` on the base task are
therefore for deployments behind a reverse proxy or an API gateway, and both are optional.

### Project structure

```
plugin-quickwit/
├── src/main/java/io/kestra/plugin/quickwit/
│   ├── AbstractQuickwitTask.java     # shared connection properties
│   ├── QuickwitService.java          # endpoints, requests, errors
│   ├── models/                       # API response POJOs
│   ├── search/  ingest/  index/  source/  deletetask/
│   └── package-info.java
├── src/main/resources/
│   ├── doc/io.kestra.plugin.quickwit.md
│   ├── icons/                        # plugin-icon.svg + one per subpackage
│   └── metadata/                     # index.yaml + one per subpackage
├── src/test/java/io/kestra/plugin/quickwit/
├── build.gradle
└── README.md
```

## Local rules

- Base the wording on the implemented packages and classes, not on template README text.
- Every task and the trigger need a `@Plugin` example with `full = true`, valid YAML containing
  `id`, `namespace` and `tasks` or `triggers`. Secrets go through `{{ secret('NAME') }}`.
- Every property and output field needs a `@Schema`; a `@Schema(title = ...)` must not end with a period.
- `@PluginProperty(group = ...)` accepts only: `main`, `connection`, `source`, `processing`,
  `execution`, `destination`, `reliability`, `advanced`, `deprecated`.
- A property may not be named `version`: Kestra reserves it to pin a plugin version. Quickwit's
  configuration format version is therefore `configVersion`.
- The `index` subpackage metadata file must keep its dotted name
  (`io.kestra.plugin.quickwit.index.yaml`). A leaf-named `index.yaml` would collide with the root
  metadata file, which is why `META-004` is disabled in `build.gradle`; the invariant it would have
  checked is enforced by `MetadataConsistencyTest`.
- Tests use WireMock (`org.wiremock:wiremock-jetty12`) with `@KestraTest`; no live Quickwit is needed.
  `QuickwitContainerTest` is the one exception: a single create-ingest-search round-trip against a real
  node via Testcontainers, aborting where Docker is unavailable. Point `QUICKWIT_IT_URL` at a running
  node (e.g. `http://localhost:7280`) to run it without Docker.
  Do not name a test method after a WireMock DSL method (`get`, `delete`, `list`): it hides the
  static import and the DSL call silently resolves to the test method.
- Run `./gradlew build` before pushing. It runs `lintPluginDocs`, the tests and JaCoCo.

## References

- https://quickwit.io/docs
- https://quickwit.io/docs/reference/rest-api
- https://quickwit.io/docs/reference/query-language
- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines