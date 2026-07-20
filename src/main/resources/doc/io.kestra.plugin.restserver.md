The REST Server plugin embeds a declarative HTTP server as a **realtime trigger**. Routes are defined in
YAML, in the spirit of Apache Camel's REST DSL, and every incoming HTTP request starts one Kestra
execution with the request data exposed as `{{ trigger.* }}` variables.

## How it works

`RestServerRealtimeTrigger` starts an embedded HTTP server on the worker that runs the trigger and
registers the routes you declare. Because Kestra executions are asynchronous, each matching request is
answered immediately with `202 Accepted` and the generated execution id — the response does not wait for
the flow to finish.

| Situation | Response | Execution created? |
|-----------|----------|--------------------|
| Request matches a route | `202 Accepted` + execution id | yes |
| Path or method matches no route | `404 Not Found` | no |
| `Content-Type` violates route `consumes` | `415 Unsupported Media Type` | no |

## Example

```yaml
id: order-api
namespace: company.myapp

tasks:
  - id: handle_request
    type: io.kestra.plugin.core.log.Log
    message: "Received {{ trigger.method }} {{ trigger.path }} with body {{ trigger.body }}"

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
```

## Operational notes

- **The port is bound on the worker.** A realtime trigger runs on one worker, so the declared port must be
  free on that host and reachable by your callers. Two flows cannot share a port on the same worker.
- **Nothing here authenticates callers.** Keep the port on an internal network or behind a reverse proxy
  that handles TLS and authentication.
- **Route changes take effect on trigger restart.** Routes are rendered and registered once, when the
  server starts.
