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

| Plugin  | Kestra   | Javalin | Jetty    | Java |
|---------|----------|---------|----------|------|
| `1.1.0` | `1.3.28` | `7.2.2` | `12.1.8` | 21+  |
| `1.0.0` | `1.3.28` | `7.2.2` | `12.1.8` | 21+  |

To build against a different Kestra version, set `kestraVersion` in `gradle.properties` and, if that
version ships a different Jetty, realign `javalinVersion`/`jettyVersion` as described in the notes below.
When cutting a new release, add a row here for the versions it was built against.

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
| `waitTimeout`    | `Duration`    | `PT30S`     | How long a waiting request blocks before returning `504`.         |
| `responseOutput` | `String`      | `response`  | Flow output whose `{status, body, headers}` shapes the response.  |
| `authHeader`     | `String`      | `X-Api-Key` | Header carrying the API key (case-insensitive lookup).            |
| `apiKey`         | `String`      | _(none)_    | Expected API key; when set, requests without it get `401`. Empty/null disables auth. Store it as a secret. |

Each route takes `method` (required), `path` (required), `consumes`, `produces`, and an optional `wait`
that overrides the trigger-level default. All of these are Kestra properties, so they accept Pebble
expressions.

### Trigger outputs

| Variable               | Type                  | Description                                    |
|------------------------|-----------------------|------------------------------------------------|
| `trigger.method`       | `String`              | `GET`, `POST`, …                               |
| `trigger.path`         | `String`              | Actual request path, e.g. `/api/orders/42`.    |
| `trigger.matchedRoute` | `String`              | Route pattern, e.g. `/api/orders/{id}`.        |
| `trigger.pathParams`   | `Map<String, String>` | Parameters extracted from the URL.             |
| `trigger.queryParams`  | `Map<String, String>` | Repeated parameters are joined with a comma.   |
| `trigger.headers`      | `Map<String, String>` | Request headers.                               |
| `trigger.body`         | `String`              | Raw body, decoded as a string.                 |
| `trigger.contentType`  | `String`              | `Content-Type` of the request.                 |

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

Synchronous mode observes the in-process execution queue. It is validated on the standalone (`server local`)
runner; on distributed executor backends its behaviour needs separate verification.

### Authentication

Set `apiKey` (from a secret) to require an API key. Requests missing it, or presenting the wrong value in
the `authHeader` header (default `X-Api-Key`), are rejected with `401` before any route matching or
execution. The header lookup is **case-insensitive**, so a gateway that normalises header casing cannot
cause a fail-open. Leaving `apiKey` empty or unset disables the check. This is a single shared key; for
per-caller auth or TLS, keep the port behind a reverse proxy.

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

Copy the shadow jar into Kestra's plugins directory:

```bash
cp build/libs/plugin-rest-server-*.jar /path/to/kestra/plugins/
```

Or use the provided Docker setup, which also publishes the plugin's port:

```bash
./gradlew shadowJar && docker compose up -d

# Kestra 1.x needs its basic-auth user created once, on first boot:
curl -X POST localhost:8080/api/v1/basicAuth \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin@kestra.io","password":"Admin1234"}'

curl -u admin@kestra.io:Admin1234 -X POST localhost:8080/api/v1/main/flows \
  -H 'Content-Type: application/x-yaml' --data-binary @examples/order-api.yaml

curl -i -X POST localhost:8090/api/orders \
  -H 'Content-Type: application/json' -d '{"item":"widget"}'
```

Note that `docker-compose.yml` mounts `build/libs` as the plugins directory, so keep only the shadow jar
there.

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

**Nothing here authenticates callers.** Anyone who can reach the port can start executions. Keep the
port on an internal network or behind a reverse proxy that handles TLS and authentication.

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

### `consumes` is enforced

The spec declares `consumes` but its sample handler never reads it. Here a mismatch is rejected with
`415` before any execution is created, which is the only behaviour that makes the field meaningful.

## Not implemented

From the spec's "Future Enhancements": TLS termination and a companion response task. Synchronous waiting
(`wait`), flow-controlled responses (`responseOutput`), and API-key auth (`apiKey`) are now supported — see
the sections above. TLS and per-caller auth are still expected to be handled by a reverse proxy.
