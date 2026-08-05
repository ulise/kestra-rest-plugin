# Kestra Plugin: REST Server Realtime Trigger

[![Main](https://github.com/ulise/kestra-rest-plugin/actions/workflows/main.yml/badge.svg)](https://github.com/ulise/kestra-rest-plugin/actions/workflows/main.yml)
[![Release](https://img.shields.io/github/v/release/ulise/kestra-rest-plugin?sort=semver)](https://github.com/ulise/kestra-rest-plugin/releases/latest)

A Kestra plugin that embeds a declarative HTTP server as a **realtime trigger**. Routes are defined in
YAML, in the spirit of Apache Camel's REST DSL, and every incoming HTTP request starts one Kestra
execution with the request data exposed as `{{ trigger.* }}` variables.

- **Coordinates:** `io.kestra.plugin:plugin-rest-server`
- **Package:** `io.kestra.plugin.restserver`
- **Requires:** Java 21+, Kestra 1.3.x

## Compatibility

Each plugin release is built and tested against a specific Kestra version. Pick the release matching
your instance; the plugin embeds Javalin on a Jetty aligned to that Kestra's Jetty (see
[Javalin 7 instead of 6, and a pinned Jetty](#javalin-7-instead-of-6-and-a-pinned-jetty)).

| Plugin  | Kestra   | Javalin | Jetty     | Java |
|---------|----------|---------|-----------|------|
| `1.4.2` | `1.3.31` | `7.2.2` | `12.1.10` | 21+  |
| `1.4.1` | `1.3.28` | `7.2.2` | `12.1.10` | 21+  |
| `1.4.0` | `1.3.28` | `7.2.2` | `12.1.8`  | 21+  |
| `1.3.0` | `1.3.28` | `7.2.2` | `12.1.8`  | 21+  |
| `1.2.0` | `1.3.28` | `7.2.2` | `12.1.8`  | 21+  |
| `1.1.3` | `1.3.28` | `7.2.2` | `12.1.8`  | 21+  |
| `1.1.2` | `1.3.28` | `7.2.2` | `12.1.8`  | 21+  |
| `1.1.1` | `1.3.28` | `7.2.2` | `12.1.8`  | 21+  |
| `1.1.0` | `1.3.28` | `7.2.2` | `12.1.8`  | 21+  |
| `1.0.0` | `1.3.28` | `7.2.2` | `12.1.8`  | 21+  |

`1.4.0` was additionally verified end to end against a running `kestra/kestra:v1.3.28`, installing the
published artifact rather than a local build: a multipart file part and a `fetchType: STORE` body both read
back **byte-identical** from the internal storage and were consumed by `io.kestra.plugin.core.storage.Size`.

To build against a different Kestra version, set `kestraVersion` in `gradle.properties` and, if that
version ships a different Jetty, realign `javalinVersion`/`jettyVersion` as described in the notes below.
When cutting a new release, add a row here for the versions it was built against.

### Upgrading to 1.4.0

Uploads are no longer base64-encoded into the execution. **`trigger.parts[].content` is gone**, replaced by
`trigger.parts[].uri`, a `kestra://` URI pointing at the part in Kestra's internal storage; a flow reading
`content` fails to render rather than silently seeing nothing. `base64Body`/`trigger.bodyBase64` still work
and are deprecated in favour of `fetchType: STORE`. See
[Binary and multipart bodies](#binary-and-multipart-bodies), and
[Reading a stored part inside a script task](#reading-a-stored-part-inside-a-script-task) if the flow decoded
`content` in a script — that is the migration path for it.

## Usage

```yaml
id: order-api
namespace: company.myapp

tasks:
  - id: handle_request
    type: io.kestra.plugin.core.log.Log
    message: |
      Method:       {{ trigger.method }}
      Path:         {{ trigger.path }}
      MatchedRoute: {{ trigger.matchedRoute }}
      PathParams:   {{ trigger.pathParams }}
      QueryParams:  {{ trigger.queryParams }}
      Body:         {{ trigger.body }}

triggers:
  - id: rest_server
    type: io.kestra.plugin.restserver.RestServerRealtimeTrigger
    port: 8090
    basePath: /api
    routes:
      - method: GET
        path: /orders/{id}
        produces: application/json
      - method: POST
        path: /orders
        consumes: application/json
        produces: application/json
      - method: DELETE
        path: /orders/{id}
```

```console
$ curl -i -X POST localhost:8090/api/orders -H 'Content-Type: application/json' -d '{"item":"widget"}'
HTTP/1.1 202 Accepted
Content-Type: application/json

{"status":"accepted","executionId":"5cVhZFvXQBAxTGqYbJmpKe"}
```

### Trigger properties

| Property         | Type          | Default     | Description                                                        |
|------------------|---------------|-------------|--------------------------------------------------------------------|
| `port`           | `Integer`     | `8080`      | Port the embedded server listens on.                               |
| `host`           | `String`      | `0.0.0.0`   | Interface to bind to.                                              |
| `basePath`       | `String`      | `/`         | Prefix prepended to every route.                                  |
| `routes`         | `List<Route>` | required    | Routes to serve; must not be empty.                               |
| `wait`           | `Boolean`     | `false`     | Default sync mode for every route (see [Synchronous mode](#synchronous-mode-and-flow-controlled-responses)). |
| `fetchType`      | `FETCH`\|`STORE`\|`NONE` | `FETCH` | Default handling of the request body for every route (see [Binary and multipart bodies](#binary-and-multipart-bodies)). |
| `waitTimeout`    | `Duration`    | `PT30S`     | How long a waiting request blocks before returning `504`.         |
| `responseOutput` | `String`      | `response`  | Flow output whose `{status, body, headers}` shapes the response.  |
| `authHeader`     | `String`      | `X-Api-Key` | Header carrying the API key (case-insensitive lookup).            |
| `apiKey`         | `String`      | _(none)_    | Expected API key; when set, requests without it get `401`. Empty/null disables auth. Store it as a secret. |
| `apiKeys`        | `List<String>`| _(none)_    | Accepted keys for multiple callers; a request passes if its key matches `apiKey` or any entry. The matched key still reaches the flow. Store as secrets. |
| `basicAuth`      | `List<Credential>` | _(none)_ | Accepted HTTP Basic `{username, password}` pairs (see [HTTP Basic](#http-basic)). Store as secrets. |
| `invalidCredentialsStatus` | `Integer` | `401`   | Status when credentials are present but wrong; set `403` to keep a caller contract that separates the two. |
| `authFailureBody`| `String`      | _(none)_    | Body returned verbatim on an authentication failure, e.g. `{}`. Defaults to the plugin's own JSON. |
| `maxRequestSize` | `Long`        | `10485760`  | Maximum request size in bytes (incl. multipart uploads). Defaults to 10 MB. |

Each route takes `method` (required), `path` (required), `consumes`, `produces`, and optional `wait` and
`fetchType` that override the trigger-level defaults (see [Binary and multipart bodies](#binary-and-multipart-bodies)).
`base64Body` is still accepted but deprecated. All of these are Kestra properties, so they accept Pebble expressions.

### Trigger outputs

| Variable               | Type                  | Description                                    |
|------------------------|-----------------------|------------------------------------------------|
| `trigger.method`       | `String`              | `GET`, `POST`, …                               |
| `trigger.path`         | `String`              | Actual request path, e.g. `/api/orders/42`.    |
| `trigger.matchedRoute` | `String`              | Route pattern, e.g. `/api/orders/{id}`.        |
| `trigger.pathParams`   | `Map<String, String>` | Parameters extracted from the URL.             |
| `trigger.queryParams`  | `Map<String, String>` | Repeated parameters are joined with a comma.   |
| `trigger.headers`      | `Map<String, String>` | Request headers. `Authorization` is omitted when `basicAuth` is set. |
| `trigger.basicAuthUser`| `String`              | Username from the request's Basic credentials; only when `basicAuth` is set. |
| `trigger.body`         | `String`              | Raw body, decoded as a string; only for `fetchType: FETCH` (non-multipart). |
| `trigger.uri`          | `String`              | `kestra://` URI of the stored body; only for `fetchType: STORE`, and only when a body was sent. |
| `trigger.bodyBase64`   | `String`              | Raw body base64-encoded; only when the route sets the deprecated `base64Body: true`. |
| `trigger.parts`        | `List<Part>`          | Uploaded file parts of a multipart request; each `Part` is `{name, filename, contentType, size, uri}`, the content stored in Kestra's internal storage. |
| `trigger.formFields`   | `Map<String,List<String>>` | Non-file form fields of a multipart request. |
| `trigger.contentType`  | `String`              | `Content-Type` of the request.                 |

### Binary and multipart bodies

`trigger.body` is a UTF-8 string, which corrupts binary content. Two things carry bytes safely, and neither
puts them in the execution record.

**`multipart/form-data` is detected automatically.** Every file part is streamed into Kestra's internal
storage as it arrives and exposed as `trigger.parts` — `{name, filename, contentType, size, uri}` — with the
non-file fields as `trigger.formFields`. Hand `uri` to any task that takes a file:

```yaml
tasks:
  - id: store_photo
    type: io.kestra.plugin.core.storage.Concat
    files:
      - "{{ trigger.parts[0].uri }}"
```

Parts are stored under the execution the request creates (`…/executions/{id}/webhook/{n}/{filename}`) and are
purged with it. They are numbered because two parts of one request may share a filename, and only the file
name of a caller-supplied path is kept, so a part called `../../evil.jpg` cannot escape its directory. A name
holding characters a URI reserves — a space, as in `My Report.pdf` — is percent-encoded in `uri` and stored
under the name the caller gave it; `trigger.parts[].filename` always reports that name unencoded.

**`fetchType` decides what happens to a non-multipart body**, on the trigger or per route:

| `fetchType` | Effect |
|-------------|--------|
| `FETCH` (default) | The body reaches the flow as `{{ trigger.body }}`, decoded as a string. Unchanged behaviour. |
| `STORE` | The body is streamed into the internal storage as it is received, and reached as `{{ trigger.uri }}`. Nothing of it is held in memory or written into the execution. Use it for uploads of any size and for binary bodies. |
| `NONE` | The body is read off the connection and dropped. |

```yaml
triggers:
  - id: rest_server
    type: io.kestra.plugin.restserver.RestServerRealtimeTrigger
    port: 8090
    basePath: /api
    fetchType: FETCH             # trigger-wide default
    routes:
      - method: POST
        path: /feedback          # JSON or multipart with result photos
        produces: application/json
      - method: POST
        path: /images
        fetchType: STORE         # binary image body as {{ trigger.uri }}
```

A request rejected before its body is read — by authentication or by `consumes` — stores nothing, and a
request that fails after storing has what it stored deleted, since no execution will exist to be purged.

Uploads are capped by `maxRequestSize` (default 10 MB); parts above 1 MB are buffered to a temporary file by
Jetty rather than held in the heap.

> **Deprecated: `base64Body`.** Setting it on a route additionally exposes `{{ trigger.bodyBase64 }}`, the
> base64 of the raw bytes, as releases before 1.4.0 did. It still works when the body is fetched, and is
> ignored for `fetchType: STORE`/`NONE`. Prefer `STORE`: base64 inflates the payload by a third and puts all
> of it in the execution record. Note also that Pebble's `| base64decode` returns a UTF-8 `String`, so it
> round-trips a text body but **corrupts** a binary one — there is no pure-YAML way back to the bytes.

### Reading a stored part inside a script task

A stored part is a `kestra://` URI, and any task that takes a file resolves it. A **script** task is the
exception worth spelling out: the code runs in a subprocess — the Process runner, or a container under the
Docker runner — which has no access to Kestra's internal storage and cannot dereference the URI itself. The
bytes have to be handed in, and `inputFiles` is what does it. Kestra fetches each `kestra://` value into the
task's working directory, where the script just opens it by name.

With a known part, name it directly:

```yaml
tasks:
  - id: handle
    type: io.kestra.plugin.scripts.python.Script
    inputFiles:
      photo.jpg: "{{ trigger.parts[0].uri }}"     # same for a STORE body: {{ trigger.uri }}
    script: |
      with open("photo.jpg", "rb") as fh:
          data = fh.read()
```

When the part count is decided by the caller, render the map instead. `inputFiles` accepts a **JSON string**,
which Kestra renders before parsing, so a Pebble loop can build it:

```yaml
    inputFiles: |
      { {% for part in trigger.parts | default([]) %}{% if not loop.first %}, {% endif %}"upload-{{ loop.index }}": {{ part.uri | toJson }}{% endfor %} }
    script: |
      import json
      parts = json.loads(r'''{{ (trigger.parts | default([])) | toJson }}''')
      for index, part in enumerate(parts):
          with open(f"upload-{index}", "rb") as fh:
              data = fh.read()
          # part["filename"], part["contentType"], part["size"] describe these bytes
```

Four things make that work, and each fails quietly if ignored:

- **The key carries the pairing.** `loop.index` is 0-based, so `upload-<n>` is the part's position in
  `trigger.parts`, and `enumerate()` over the same list re-derives it. Filter the list on the Python side
  (skipping a part, say) and the indices still line up only if you keep the original index — filter into
  `[(i, p) for i, p in enumerate(parts) if …]`, never into a re-indexed list. Get this wrong and images are
  attributed to the wrong metadata, which no error surfaces.
- **`| toJson` quotes and escapes the URI** rather than concatenating it, so a caller-chosen filename cannot
  break out of the JSON string. Kestra renders only the *keys* of `inputFiles` a second time, not the values,
  so a filename containing `{{ … }}` is inert.
- **`| default([])` covers every other request.** The same task serves the routes that are not multipart, and
  there `trigger.parts` is absent; the template then renders `{ }`, an empty map that stages nothing.
- **Parts stay where they were stored.** Passing `part.uri` on to a subflow is cheaper than copying the bytes
  through an `outputFile`, and the file lives until the execution is purged.

> ⚠️ **Never write the literal `kestra:` + `//` scheme in the script text — not even in a comment.** Kestra
> scans the rendered script for internal-storage URIs and calls `URI.create` on every match, so a bare scheme
> fails the task with `IllegalArgumentException: Expected authority at index 9` before the first line runs.
> If the trace also shows `Provided N input(s).`, the staging already succeeded and the fault is in the script
> text, not in `inputFiles`.

### Response semantics

By default executions are asynchronous, so the server answers immediately rather than waiting for the flow:

| Situation                                | Response                      | Execution created? |
|------------------------------------------|-------------------------------|--------------------|
| Missing/wrong `apiKey` (when configured) | `401 Unauthorized`            | no                 |
| Request matches a route                  | `202 Accepted` + execution id | yes                |
| Path or method matches no route          | `404 Not Found`               | no                 |
| `Content-Type` violates route `consumes` | `415 Unsupported Media Type`  | no                 |

`consumes` is compared on the media type only, so `application/json; charset=utf-8` satisfies
`application/json`. In async mode, to retrieve the result poll `GET /api/v1/executions/{executionId}` on
the Kestra API — or use synchronous mode below.

### Synchronous mode and flow-controlled responses

Set `wait: true` (per trigger or per route) to serve a request/response API. The request then blocks until
the triggered execution reaches a terminal state, and the HTTP response is built from a flow output (named
by `responseOutput`, default `response`):

```yaml
outputs:
  - id: response
    type: JSON                        # required by Kestra; the value below is the response map
    value:
      status: 404                     # HTTP status; default 200 on success, 500 on failure
      body: '{"status":"NOT_FOUND"}'  # a string is returned verbatim; an object is serialised as JSON
      headers:                        # optional
        X-Trace-Id: "{{ execution.id }}"
```

The flow-produced `body` is returned **verbatim for every status, including non-2xx** — so two `404`s can
carry different bodies (e.g. `{"status":"NOT_FOUND"}` vs `{"status":"NO_RECEIPT"}`). When the output is
absent, a successful execution returns `200` with its outputs as JSON and a failed one returns `500`. If
the execution does not finish within `waitTimeout`, the request returns `504` (the execution keeps running).

Synchronous mode requires the trigger's worker and the executor to share a JVM — the case for
`kestra server local`, `kestra server standalone`, and a single-replica Helm `standalone` deployment. It is
**queue-backend agnostic**: the trigger subscribes to the execution queue through Kestra's `QueueInterface`,
so the memory, H2, MySQL and Postgres queues all work. Verified on both H2 (`server local`) and postgres
(`server standalone`) — see [Testing against postgres](#testing-against-postgres).

On JDBC backends the queue is polled rather than dispatched in-process, which costs latency on an
otherwise idle instance: measured against postgres, a request arriving while the poller is hot returns in
~0.1–0.3 s, one arriving after 60 s of silence in ~0.8 s. That gap is
`kestra.jdbc.queues.max-poll-interval` (default 500 ms); lower it if it matters for your endpoint.

Distributed deployments are **not** supported, and the reason is the HTTP server rather than the queue: a
realtime trigger is evaluated on exactly one worker, so only that worker binds the port. Scaling past one
replica leaves the other replicas with nothing listening. A worker restart or trigger rebalance likewise
drops in-flight waiting requests, which a synchronous caller sees as a dropped connection.

### Authentication

Set `apiKey` (from a secret) to require an API key. Requests missing it, or presenting the wrong value in
the `authHeader` header (default `X-Api-Key`), are rejected with `401` before any route matching or
execution. The header lookup is **case-insensitive**, so a gateway that normalises header casing cannot
cause a fail-open. Leaving both `apiKey` and `apiKeys` empty or unset disables the check. For callers that
send `Authorization: Basic …` instead, see [HTTP Basic](#http-basic). For TLS, keep the port behind a
reverse proxy.

**Multiple callers.** To front several partners that each present their own key, list the accepted keys in
`apiKeys` (combined with `apiKey`); a request passes if its key matches any of them. The **matched key is
still forwarded to the flow** in `{{ trigger.headers }}`, so the flow can map the caller to their data —
the plugin only gates on membership, it never strips the key:

```yaml
triggers:
  - id: rest_server
    type: io.kestra.plugin.restserver.RestServerRealtimeTrigger
    port: 8090
    basePath: /api
    apiKeys:
      - "{{ secret('PARTNER_A_KEY') }}"
      - "{{ secret('PARTNER_B_KEY') }}"
    routes:
      - method: GET
        path: /orders/{id}
        produces: application/json
```

### HTTP Basic

For callers that authenticate with `Authorization: Basic …` and cannot be asked to switch, list the accepted
pairs in `basicAuth`. It combines with `apiKey`/`apiKeys` — a request passes if it satisfies **either** scheme —
and, like them, rejects before any route matching or execution:

```yaml
triggers:
  - id: rest_server
    type: io.kestra.plugin.restserver.RestServerRealtimeTrigger
    port: 8090
    basePath: /api
    invalidCredentialsStatus: 403   # default 401
    authFailureBody: "{}"           # default {"status":"UNAUTHORIZED"} / {"status":"FORBIDDEN"}
    basicAuth:
      - username: "{{ secret('PARTNER_A_USER') }}"
        password: "{{ secret('PARTNER_A_PASSWORD') }}"
      - username: "{{ secret('PARTNER_B_USER') }}"
        password: "{{ secret('PARTNER_B_PASSWORD') }}"
    routes:
      - method: POST
        path: /orders
```

**The password never reaches the flow.** Unlike the API key, which is deliberately forwarded, `Authorization`
is stripped from `{{ trigger.headers }}` and the matched username is exposed as `{{ trigger.basicAuthUser }}`
instead. This matters because trigger variables are persisted in the execution — into the queue table, the
execution repository, and the Kestra UI — and `Basic` is base64, not encryption, so a forwarded header would
put the caller's password in front of anyone with execution read access. Note that Kestra's
`@PluginProperty(secret = true)` is documentation metadata for UI masking, not runtime redaction, so it would
not have prevented this.

**`401` vs `403`.** By default every rejection is `401`, as RFC 9110 prescribes. Set `invalidCredentialsStatus:
403` to preserve a caller contract that distinguishes the two:

| Request | Status |
|---|---|
| No `Authorization` header, another scheme (`Bearer …`), undecodable base64, or no `:` in the decoded value | `401` — nothing could be compared |
| Well-formed `Basic` credentials that match no configured pair | `invalidCredentialsStatus` |

`401` responses carry `WWW-Authenticate: Basic` when `basicAuth` is configured; `403` responses do not, since a
`403` is not a challenge. Be aware the challenge header makes a browser pop a native login dialog — harmless for
machine-to-machine traffic, surprising if someone opens the URL by hand.

The scheme token is matched **case-insensitively** (`Basic`, `basic`, `BASIC`) per RFC 9110, and credentials are
compared in constant time against every configured pair. Credentials are decoded as UTF-8 per RFC 7617; a legacy
client that encodes a non-ASCII password as ISO-8859-1 will not match, though ASCII credentials — the
overwhelmingly common case — are unaffected.

### Letting the flow own authentication

If you prefer the flow to own auth entirely (arbitrary key-to-partner logic, lookups against a database), leave
`apiKey`, `apiKeys` and `basicAuth` unset and validate `{{ trigger.headers }}` in the flow, returning a
flow-controlled `401` via `responseOutput` (requires `wait: true`). The cost is that **every request creates an
execution before it is rejected**, so unauthenticated traffic reaches the executor and shows up in the execution
list — which is exactly what the edge checks above avoid.

## Build

```bash
./gradlew build          # compile, test, package
./gradlew shadowJar      # build/libs/plugin-rest-server-<version>.jar
```

The unit tests start the real server on an ephemeral port and drive it over HTTP; they need no Kestra
instance. Beyond those, the plugin has been verified end-to-end against Kestra 1.3.28 in Docker: the
plugin loads, the flow above deploys, and `202` / `404` / `415` and the trigger variables all behave as
documented.

## Deploy

Install by coordinates from the public Maven repository (served from the `gh-pages` branch, no credentials
required) — the cleanest option for a custom image:

```dockerfile
FROM kestra/kestra:1.3.28
RUN /app/kestra plugins install io.kestra.plugin:plugin-rest-server:1.1.2 \
      --repositories https://ulise.github.io/kestra-rest-plugin/maven
```

Or copy the shadow jar into Kestra's plugins directory:

```bash
cp build/libs/plugin-rest-server-*.jar /path/to/kestra/plugins/
```

Or use the provided Docker setup, which also publishes the plugin's port:

```bash
./gradlew shadowJar && docker compose up -d

curl -u 'admin@kestra.io:Admin1234!' -X POST localhost:8080/api/v1/main/flows \
  -H 'Content-Type: application/x-yaml' --data-binary @examples/order-api.yaml

curl -i -X POST localhost:8090/api/orders \
  -H 'Content-Type: application/json' -d '{"item":"widget"}'
```

Note that `docker-compose.yml` mounts `build/libs` as the plugins directory, so keep only the shadow jar
there.

### Testing against postgres

That default stack is `server local`, which runs on H2. `docker-compose.yml` also carries a `postgres`
profile — `server standalone` with `kestra.queue.type: postgres`, the default production shape of an OSS
install — on separate ports and volumes so both stacks can run side by side:

```bash
./gradlew shadowJar && docker compose --profile postgres up -d kestra-postgres

curl -u 'admin@kestra.io:Admin1234!' -X POST localhost:8081/api/v1/main/flows \
  -H 'Content-Type: application/x-yaml' --data-binary @examples/sync-api.yaml

curl -i localhost:8091/api/orders/42   # 200, {"id":"42","status":"FOUND"}
curl -i localhost:8091/api/orders/0    # 404, {"id":"0","status":"NOT_FOUND"}
```

Use it for anything queue-dependent — above all [synchronous mode](#synchronous-mode-and-flow-controlled-responses),
which is the only feature whose behaviour could plausibly differ per backend. `examples/sync-api.yaml`
exercises `wait: true` including the flow-controlled non-2xx path.

The images are pinned to the Kestra version in the [compatibility table](#compatibility) rather than
`latest`, which currently resolves to a 2.x nightly this plugin is not built against.

**When bind-mounting the plugins directory into a container, the source must be a path the Docker daemon
can actually see.** A directory the daemon cannot read (for example under `/tmp` on some setups, or on a
host the daemon runs remotely from) mounts as *empty* rather than failing. Kestra then silently scans zero
plugins — you'll see `Registered 0 plugins from 0 groups` in the logs and every flow using the trigger
fails with an "unknown type" error. Confirm the jar is present inside the container with
`docker exec <container> ls /app/plugins` before assuming the plugin itself is at fault.

## Operational notes

**The port is bound on the worker.** A realtime trigger runs on one worker, so the declared port must
be free on that host and reachable by your callers. With several workers, the trigger runs on whichever
one picks it up — put a load balancer in front, or pin it with a `workerGroup`. Two flows cannot share
a port on the same worker; the second trigger fails to bind.

**Authentication is opt-in, and there is no TLS.** With `apiKey`, `apiKeys` and `basicAuth` all unset, anyone
who can reach the port can start executions. Even with them set, credentials cross the wire in the clear, so
keep the port on an internal network or behind a reverse proxy that terminates TLS.

**Route changes take effect on trigger restart.** Routes are rendered and registered once, when the
server starts.

## Notes on this implementation

This plugin follows `kestra-rest-server-plugin-spec.md`, with three deviations that the spec's own
"check the Kestra source for the correct overload" caveat anticipated.

### Kestra 1.3 instead of 0.20

The spec targets Kestra 0.20, which is several major versions behind. Two APIs changed:

- Plugin properties are now `Property<T>`, rendered via `runContext.render(prop).as(Class)`, rather than
  plain fields. This is what makes route fields templatable.
- `TriggerService.generateRealtimeExecution` takes the trigger first, not the trigger context:
  `generateRealtimeExecution(this, conditionContext, triggerContext, output)`.

Set `kestraVersion` in `gradle.properties` to match your instance.

### Javalin 7 instead of 6, and a pinned Jetty

The spec calls for Javalin 6.x. Javalin 6 is built on **Jetty 11**; Kestra's platform BOM pins **Jetty
12**. Mixing them fails at server start with `NoSuchMethodError` inside Jetty. Javalin 7 is built on
Jetty 12, so this plugin uses it. The Javalin 7 API differs in three places from the spec's snippets:
routes are registered through `config.routes` inside `Javalin.create`, `HandlerType` is a record rather
than an enum, and the banner flag moved to `config.startup`.

Kestra's BOM pins Jetty with a `strictly` constraint, which would downgrade Javalin's Jetty and break
it the same way. Since Kestra core has no Jetty dependency of its own, `build.gradle` overrides that
constraint to Javalin's Jetty version. **When bumping `javalinVersion`, update `jettyVersion` to match**
the `jetty.version` property of the corresponding `io.javalin:javalin-parent` POM.

Since `1.4.1`, `jettyVersion` is deliberately one patch **ahead** of that rule: Javalin 7.2.2 is built
on Jetty `12.1.8`, and 7.2.2 is the newest Javalin 7 there is, so no Javalin upgrade brings the fix for
**CVE-2026-10050** along. That CVE is in `jetty-security`'s Digest authentication, which this plugin
never reaches — authentication is implemented in-plugin, and nothing here wires a `SecurityHandler`,
`LoginService` or `DigestAuthenticator`. But the shadow jar ships `jetty-security`, so every downstream
image scan reported a HIGH against the plugin. Staying inside `12.1.x` keeps the mixing safe; the full
suite passes on `12.1.10`, including the multipart and synchronous-mode tests that cross the
Javalin/Jetty boundary. Once Javalin publishes a release built on `12.1.10` or later, drop back to
tracking `javalin-parent`.

### `consumes` is enforced

The spec declares `consumes` but its sample handler never reads it. Here a mismatch is rejected with
`415` before any execution is created, which is the only behaviour that makes the field meaningful.

## Not implemented

From the spec's "Future Enhancements": TLS termination and a companion response task. Synchronous waiting
(`wait`), flow-controlled responses (`responseOutput`), API-key auth (`apiKey`) and HTTP Basic auth
(`basicAuth`) are now supported — see
the sections above. TLS and per-caller auth are still expected to be handled by a reverse proxy.
