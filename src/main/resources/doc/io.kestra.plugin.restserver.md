The REST Server plugin embeds a declarative HTTP server as a **realtime trigger**. Routes are defined in
YAML, in the spirit of Apache Camel's REST DSL, and every incoming HTTP request starts one Kestra
execution with the request data exposed as `{{ trigger.* }}` variables.

## How it works

`RestServerRealtimeTrigger` starts an embedded HTTP server on the worker that runs the trigger and
registers the routes you declare. By default each matching request is answered immediately with
`202 Accepted` and the generated execution id — the response does not wait for the flow to finish.

| Situation | Response | Execution created? |
|-----------|----------|--------------------|
| Missing/wrong `apiKey` (when configured) | `401 Unauthorized` | no |
| Request matches a route | `202 Accepted` + execution id | yes |
| Path or method matches no route | `404 Not Found` | no |
| `Content-Type` violates route `consumes` | `415 Unsupported Media Type` | no |

## Synchronous mode

Set `wait: true` (per trigger or per route) to serve a request/response API. The request then blocks until
the triggered execution reaches a terminal state, and the HTTP status, body, and headers are taken from the
flow output named by `responseOutput` (default `response`), which the flow fully controls — including on
non-2xx statuses. If the execution does not finish within `waitTimeout`, the request returns `504`.

```yaml
outputs:
  - id: response
    type: JSON
    value:
      status: 404
      body: '{"status":"NOT_FOUND"}'
      headers:
        X-Trace-Id: "{{ execution.id }}"
```

## Authentication

Set `apiKey` (from a secret) to require an API key in the `authHeader` header (default `X-Api-Key`,
looked up case-insensitively). Missing or wrong keys are rejected with `401` before any route matching.
Leaving both `apiKey` and `apiKeys` empty or unset disables the check.

To front several partners that each use their own key, list them in `apiKeys` (combined with `apiKey`); a
request passes if its key matches any of them. The matched key is still forwarded to the flow in
`{{ trigger.headers }}`, so the flow can map the caller to their data.

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
- **Auth is a single optional API key.** Set `apiKey` for `X-Api-Key` checks; there is no per-caller auth
  or TLS, so keep the port on an internal network or behind a reverse proxy that handles those.
- **Route changes take effect on trigger restart.** Routes are rendered and registered once, when the
  server starts.
