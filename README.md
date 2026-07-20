# Kestra Plugin: REST Server Realtime Trigger

[![Build](https://github.com/ulise/kestra-rest-plugin/actions/workflows/build.yml/badge.svg)](https://github.com/ulise/kestra-rest-plugin/actions/workflows/build.yml)

A Kestra plugin that embeds a declarative HTTP server as a **realtime trigger**. Routes are defined in
YAML, in the spirit of Apache Camel's REST DSL, and every incoming HTTP request starts one Kestra
execution with the request data exposed as `{{ trigger.* }}` variables.

- **Coordinates:** `io.kestra.plugin:plugin-rest-server`
- **Package:** `io.kestra.plugin.restserver`
- **Requires:** Java 21+, Kestra 1.3.x

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

| Property   | Type                | Default   | Description                                       |
|------------|---------------------|-----------|---------------------------------------------------|
| `port`     | `Integer`           | `8080`    | Port the embedded server listens on.              |
| `host`     | `String`            | `0.0.0.0` | Interface to bind to.                             |
| `basePath` | `String`            | `/`       | Prefix prepended to every route.                  |
| `routes`   | `List<Route>`       | required  | Routes to serve; must not be empty.               |

Each route takes `method` (required), `path` (required), `consumes` and `produces`. All of these are
Kestra properties, so they accept Pebble expressions.

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

Kestra executions are asynchronous, so the server answers immediately rather than waiting for the flow:

| Situation                                | Response                    | Execution created? |
|------------------------------------------|-----------------------------|--------------------|
| Request matches a route                  | `202 Accepted` + execution id | yes              |
| Path or method matches no route          | `404 Not Found`             | no                 |
| `Content-Type` violates route `consumes` | `415 Unsupported Media Type`| no                 |

`consumes` is compared on the media type only, so `application/json; charset=utf-8` satisfies
`application/json`. To retrieve the result, poll `GET /api/v1/executions/{executionId}` on the Kestra API.

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

From the spec's "Future Enhancements": `waitForCompletion`, `responseMapping`, TLS, auth middleware, and
a companion response task. The response is always a fixed `202` acknowledgement.
