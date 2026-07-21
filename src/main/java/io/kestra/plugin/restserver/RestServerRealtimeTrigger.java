package io.kestra.plugin.restserver;

import io.javalin.Javalin;
import io.javalin.config.SizeUnit;
import io.javalin.http.Context;
import io.javalin.http.HandlerType;
import io.javalin.http.UploadedFile;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.RealtimeTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuperBuilder
@ToString
@EqualsAndHashCode(callSuper = false)
@Getter
@NoArgsConstructor
@Schema(
    title = "Expose an HTTP API that triggers one execution per request",
    description = """
        Starts an embedded HTTP server for the lifetime of the trigger and registers the declared routes on it,
        in the spirit of Apache Camel's REST DSL. Every matching request creates one realtime execution.

        By default executions are asynchronous: the request is answered immediately with `202 Accepted` and the
        generated execution id, without waiting for the flow to finish. Enable `wait` to serve a request/response
        API instead — the request then blocks until the triggered execution reaches a terminal state and the HTTP
        status, body, and headers are taken from the execution's `responseOutput` output (see `responseOutput`),
        so the flow fully controls the response, including on non-2xx statuses.

        Requests that match no route get `404`, and requests violating a route's `consumes` get `415`; neither
        creates an execution. When `apiKey` is set, requests must present it in the `apiKeyHeader` header or get
        `401`. The server is bound on the worker that runs the trigger, so the port must be free there and
        reachable by your callers."""
)
@Plugin(
    examples = {
        @Example(
            title = "Expose a small order API and log each incoming request.",
            full = true,
            code = """
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
                """
        ),
        @Example(
            title = "Serve a synchronous read API behind an API key, with the flow controlling status and body.",
            full = true,
            code = """
                id: read-api
                namespace: company.myapp

                tasks:
                  - id: lookup
                    type: io.kestra.plugin.core.log.Log
                    message: "Looking up order {{ trigger.pathParams.id }}"

                outputs:
                  - id: response
                    type: JSON
                    value:
                      status: 404
                      body: '{"status":"NOT_FOUND"}'
                      headers:
                        X-Trace-Id: "{{ execution.id }}"

                triggers:
                  - id: rest_server
                    type: io.kestra.plugin.restserver.RestServerRealtimeTrigger
                    port: 8090
                    basePath: /api
                    wait: true
                    waitTimeout: PT30S
                    apiKey: "{{ secret('PARTNER_API_KEY') }}"
                    routes:
                      - method: GET
                        path: /orders/{id}
                        produces: application/json
                """
        )
    }
)
public class RestServerRealtimeTrigger extends AbstractTrigger
    implements RealtimeTriggerInterface, TriggerOutput<RestServerRealtimeTrigger.Output> {

    /**
     * Javalin also models lifecycle hooks such as {@code BEFORE} as handler types, and will happily invent one for
     * an unknown name. Only the real HTTP methods below may be declared on a route.
     */
    private static final Map<String, HandlerType> ALLOWED_METHODS = Stream.of(
            HandlerType.GET,
            HandlerType.POST,
            HandlerType.PUT,
            HandlerType.PATCH,
            HandlerType.DELETE,
            HandlerType.HEAD,
            HandlerType.OPTIONS
        )
        .collect(Collectors.toMap(HandlerType::name, Function.identity()));

    @Schema(title = "Port the embedded HTTP server listens on")
    @Builder.Default
    private Property<Integer> port = Property.ofValue(8080);

    @Schema(
        title = "Path prefix prepended to every route",
        description = "Use `/` to register routes at the root."
    )
    @Builder.Default
    private Property<String> basePath = Property.ofValue("/");

    @Schema(title = "Routes served by the embedded server")
    @PluginProperty(group = "main")
    @NotNull
    @NotEmpty
    private List<RouteDefinition> routes;

    @Schema(
        title = "Host interface to bind to",
        description = "Defaults to `0.0.0.0`, which accepts connections on every interface."
    )
    @Builder.Default
    private Property<String> host = Property.ofValue("0.0.0.0");

    @Schema(
        title = "Maximum request size in bytes",
        description = "Caps the request body, including `multipart/form-data` uploads. Defaults to 10 MB."
    )
    @Builder.Default
    private Property<Long> maxRequestSize = Property.ofValue(10L * 1024 * 1024);

    @Schema(
        title = "Wait for the triggered execution and return its result",
        description = """
            Default for every route unless a route sets its own `wait`. When `true`, a request blocks until the
            triggered execution reaches a terminal state, then the response is built from the execution's outputs
            (see `responseOutput`). When `false` (the default), requests are answered immediately with
            `202 Accepted` and never wait.

            Synchronous mode observes the in-process execution queue; it is validated on the standalone
            (`server local`) runner. On distributed executor backends its behaviour needs separate verification."""
    )
    @Builder.Default
    private Property<Boolean> wait = Property.ofValue(false);

    @Schema(
        title = "How long a waiting request blocks before giving up",
        description = "Applies when `wait` is enabled. On expiry the request returns `504 Gateway Timeout` and no response is built from the execution."
    )
    @Builder.Default
    private Property<Duration> waitTimeout = Property.ofValue(Duration.ofSeconds(30));

    @Schema(
        title = "Execution output that shapes the HTTP response",
        description = """
            Applies when `wait` is enabled. The named flow output should be a map with optional `status` (HTTP
            status code), `body` (a string returned verbatim, or an object serialised as JSON), and `headers`
            (a map). The body is returned for every status, including non-2xx. When the output is absent, a
            successful execution returns `200` with its outputs as JSON and a failed one returns `500`."""
    )
    @Builder.Default
    private Property<String> responseOutput = Property.ofValue("response");

    @Schema(
        title = "Header that carries the API key",
        description = "Only used when `apiKey` or `apiKeys` is set. The lookup is case-insensitive."
    )
    @Builder.Default
    private Property<String> authHeader = Property.ofValue("X-Api-Key");

    @Schema(
        title = "Expected API key",
        description = """
            When set (non-empty), every request must present this value in the `authHeader` header or it is
            rejected with `401` before any route matching or execution. Source it from a secret or KV. When null
            or empty, authentication is disabled unless `apiKeys` is set."""
    )
    @PluginProperty(secret = true, group = "connection")
    private Property<String> apiKey;

    @Schema(
        title = "Accepted API keys, for multiple callers",
        description = """
            A request is authorized when its key matches `apiKey` or any entry here — use this to front several
            partners that each present their own key. The plugin only gates on membership; the matched key still
            reaches the flow as `{{ trigger.headers }}`, so the flow can map it to the specific caller. Combined
            with `apiKey`; when both are null or empty, authentication is disabled."""
    )
    @PluginProperty(secret = true, group = "connection")
    private Property<List<String>> apiKeys;

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final CountDownLatch waitForTermination = new CountDownLatch(1);

    @Override
    public Publisher<Execution> evaluate(ConditionContext conditionContext, TriggerContext triggerContext) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        // Rendered once, before the server starts: configuration is fixed for the lifetime of the trigger, and a
        // rendering error should fail the trigger rather than an individual request.
        int rPort = runContext.render(this.port).as(Integer.class).orElse(8080);
        String rHost = runContext.render(this.host).as(String.class).orElse("0.0.0.0");
        String rBasePath = runContext.render(this.basePath).as(String.class).orElse("/");
        boolean rWaitDefault = runContext.render(this.wait).as(Boolean.class).orElse(false);
        long rMaxRequestSize = runContext.render(this.maxRequestSize).as(Long.class).orElse(10L * 1024 * 1024);
        List<CompiledRoute> compiledRoutes = compileRoutes(runContext, rBasePath, rWaitDefault);

        // The gate accepts any of apiKey plus every apiKeys entry; the matched key still reaches the flow.
        List<String> validKeys = new ArrayList<>();
        runContext.render(this.apiKey).as(String.class).filter(k -> !k.isEmpty()).ifPresent(validKeys::add);
        if (this.apiKeys != null) {
            for (String key : runContext.render(this.apiKeys).asList(String.class)) {
                if (key != null && !key.isEmpty()) {
                    validKeys.add(key);
                }
            }
        }

        HandlerConfig config = new HandlerConfig(
            runContext.render(this.authHeader).as(String.class).orElse("X-Api-Key"),
            List.copyOf(validKeys),
            runContext.render(this.responseOutput).as(String.class).orElse("response"),
            runContext.render(this.waitTimeout).as(Duration.class).orElse(Duration.ofSeconds(30))
        );

        boolean anyWait = compiledRoutes.stream().anyMatch(CompiledRoute::synchronous);

        return Flux.create(emitter -> {
            AtomicReference<Throwable> error = new AtomicReference<>();
            // Opened only when some route waits, so a purely async server never subscribes to the queue.
            ExecutionAwaiter awaiter = anyWait ? ExecutionAwaiter.open(runContext) : null;

            try {
                Javalin app = Javalin.create(config2 -> {
                    config2.startup.showJavalinBanner = false;
                    config2.startup.showOldJavalinVersionWarning = false;

                    // Raise the default 1 MB caps so real uploads (e.g. photos) are accepted.
                    config2.http.maxRequestSize = rMaxRequestSize;
                    config2.jetty.multipartConfig.maxFileSize(rMaxRequestSize, SizeUnit.BYTES);
                    config2.jetty.multipartConfig.maxTotalRequestSize(rMaxRequestSize, SizeUnit.BYTES);
                    config2.jetty.multipartConfig.maxInMemoryFileSize(
                        (int) Math.min(rMaxRequestSize, Integer.MAX_VALUE), SizeUnit.BYTES);

                    for (CompiledRoute route : compiledRoutes) {
                        config2.routes.addHttpHandler(
                            route.method(),
                            route.fullPath(),
                            ctx -> handle(ctx, route, conditionContext, triggerContext, emitter, config, awaiter)
                        );
                        logger.info("Registering route {} {}{}", route.method(), route.fullPath(), route.synchronous() ? " (wait)" : "");
                    }
                });

                emitter.onDispose(() -> {
                    try {
                        app.stop();
                    } catch (Exception e) {
                        logger.warn("Error while stopping the embedded HTTP server: {}", e.getMessage());
                    } finally {
                        if (awaiter != null) {
                            awaiter.close();
                        }
                        isActive.set(false);
                        waitForTermination.countDown();
                    }
                });

                app.start(rHost, rPort);
                logger.info("REST server listening on {}:{}", rHost, rPort);

                // Hold the trigger thread until Kestra stops us; disposal happens in onDispose above.
                busyWait();
            } catch (Throwable e) {
                error.set(e);
            } finally {
                Throwable throwable = error.get();
                if (throwable != null) {
                    emitter.error(throwable);
                } else {
                    emitter.complete();
                }
            }
        });
    }

    private void handle(
        Context ctx,
        CompiledRoute route,
        ConditionContext conditionContext,
        TriggerContext triggerContext,
        FluxSink<Execution> emitter,
        HandlerConfig config,
        ExecutionAwaiter awaiter
    ) {
        // Authentication runs before route logic, so an unauthenticated caller learns nothing about the routes.
        if (!config.validKeys().isEmpty() && !authorized(ctx.headerMap(), config.authHeader(), config.validKeys())) {
            ctx.status(401)
                .contentType("application/json")
                .result(json(Map.of("status", "UNAUTHORIZED")));
            return;
        }

        if (!matchesConsumes(ctx, route)) {
            ctx.status(415)
                .contentType("application/json")
                .result(json(Map.of(
                    "status", "rejected",
                    "error", "Unsupported Media Type, expected " + route.consumes()
                )));
            return;
        }

        Output.OutputBuilder builder = Output.builder()
            .method(ctx.method().name())
            .path(ctx.path())
            .matchedRoute(route.fullPath())
            .pathParams(ctx.pathParamMap())
            .queryParams(
                ctx.queryParamMap().entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> String.join(",", e.getValue())))
            )
            .headers(ctx.headerMap())
            .contentType(ctx.contentType());

        if (ctx.isMultipartFormData()) {
            // File bytes would not survive string decoding, so expose the parts (base64) and text fields
            // instead of the raw multipart envelope.
            builder.parts(uploadedParts(ctx)).formFields(ctx.formParamMap());
        } else {
            builder.body(ctx.body());
            if (route.base64Body()) {
                builder.bodyBase64(Base64.getEncoder().encodeToString(ctx.bodyAsBytes()));
            }
        }

        Output output = builder.build();

        // Built here rather than in a downstream map() so the caller can be told which execution it started.
        Execution execution = TriggerService.generateRealtimeExecution(this, conditionContext, triggerContext, output);

        if (!route.synchronous()) {
            emitter.next(execution);
            ctx.status(202)
                .contentType(route.produces())
                .result(json(Map.of(
                    "status", "accepted",
                    "executionId", execution.getId()
                )));
            return;
        }

        // Synchronous mode: register interest before emitting, so a fast completion cannot be missed.
        CompletableFuture<Execution> completion = awaiter.register(execution.getId());
        emitter.next(execution);

        try {
            Execution terminal = completion.get(config.waitTimeout().toMillis(), TimeUnit.MILLISECONDS);
            applyResponse(ctx, mapResponse(terminal, config.responseOutput(), route.produces()));
        } catch (TimeoutException e) {
            awaiter.cancel(execution.getId());
            ctx.status(504)
                .contentType("application/json")
                .result(json(Map.of("status", "timeout", "executionId", execution.getId())));
        } catch (Exception e) {
            awaiter.cancel(execution.getId());
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            ctx.status(500)
                .contentType("application/json")
                .result(json(Map.of("status", "error", "error", e.getMessage() != null ? e.getMessage() : e.toString())));
        }
    }

    /**
     * Case-insensitive lookup of the API key header, then a constant-time comparison against each accepted key.
     * HTTP header names are case-insensitive and gateways normalise them, so an exact-case {@code get} could
     * silently find nothing and read as "no key required" — a fail-open bug this deliberately avoids. The request
     * is authorized when the presented key matches any accepted key; every candidate is compared (no early exit)
     * so the number of comparisons does not depend on which key matched.
     */
    static boolean authorized(Map<String, String> headers, String headerName, Collection<String> expectedKeys) {
        String provided = null;
        for (Map.Entry<String, String> header : headers.entrySet()) {
            if (header.getKey().equalsIgnoreCase(headerName)) {
                provided = header.getValue();
                break;
            }
        }

        if (provided == null) {
            return false;
        }

        byte[] providedBytes = provided.getBytes(StandardCharsets.UTF_8);
        boolean match = false;
        for (String expected : expectedKeys) {
            match |= MessageDigest.isEqual(providedBytes, expected.getBytes(StandardCharsets.UTF_8));
        }

        return match;
    }

    /**
     * Maps a terminal execution to an HTTP response. When the {@code responseOutput} output is present, the flow
     * fully controls status, body, and headers (including for non-2xx). Otherwise a successful execution yields
     * {@code 200} with its outputs as JSON and a failed one yields {@code 500}.
     */
    @SuppressWarnings("unchecked")
    static ResponseSpec mapResponse(Execution execution, String responseOutputKey, String defaultContentType) throws Exception {
        Map<String, Object> outputs = execution.getOutputs();
        boolean success = execution.getState().isTerminatedNoFail();
        Object mapped = outputs == null ? null : outputs.get(responseOutputKey);

        if (mapped instanceof Map<?, ?> response) {
            int status = toStatus(response.get("status"), success);
            String body = toBody(response.get("body"));
            Map<String, String> headers = toHeaders(response.get("headers"));
            String contentType = headers.entrySet().stream()
                .filter(h -> h.getKey().equalsIgnoreCase("Content-Type"))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(defaultContentType);

            return new ResponseSpec(status, body, headers, contentType);
        }

        String body = outputs == null || outputs.isEmpty() ? "" : JacksonMapper.ofJson().writeValueAsString(outputs);
        return new ResponseSpec(success ? 200 : 500, body, Map.of(), defaultContentType);
    }

    private static int toStatus(Object status, boolean success) {
        if (status instanceof Number number) {
            return number.intValue();
        }
        if (status instanceof String string && !string.isBlank()) {
            return Integer.parseInt(string.trim());
        }

        return success ? 200 : 500;
    }

    private static String toBody(Object body) throws Exception {
        if (body == null) {
            return "";
        }
        if (body instanceof String string) {
            return string;
        }

        return JacksonMapper.ofJson().writeValueAsString(body);
    }

    private static Map<String, String> toHeaders(Object headers) {
        if (!(headers instanceof Map<?, ?> map)) {
            return Map.of();
        }

        Map<String, String> result = new LinkedHashMap<>();
        map.forEach((key, value) -> {
            if (key != null && value != null) {
                result.put(key.toString(), value.toString());
            }
        });

        return result;
    }

    private static void applyResponse(Context ctx, ResponseSpec spec) {
        ctx.status(spec.status());
        spec.headers().entrySet().stream()
            // Content-Type is set through contentType() below, so it is not duplicated here.
            .filter(h -> !h.getKey().equalsIgnoreCase("Content-Type"))
            .forEach(h -> ctx.header(h.getKey(), h.getValue()));
        ctx.contentType(spec.contentType());
        ctx.result(spec.body());
    }

    /**
     * Reads every uploaded file part into a {@link Part} with its bytes base64-encoded, so binary content
     * (images, etc.) survives the trip through Kestra's string/JSON trigger variables.
     */
    private static List<Part> uploadedParts(Context ctx) {
        List<Part> parts = new ArrayList<>();

        ctx.uploadedFileMap().forEach((name, files) -> {
            for (UploadedFile file : files) {
                byte[] bytes;
                try (InputStream in = file.content()) {
                    bytes = in.readAllBytes();
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to read uploaded part '" + name + "'", e);
                }

                parts.add(Part.builder()
                    .name(name)
                    .filename(file.filename())
                    .contentType(file.contentType())
                    .size(file.size())
                    .content(Base64.getEncoder().encodeToString(bytes))
                    .build());
            }
        });

        return parts;
    }

    private boolean matchesConsumes(Context ctx, CompiledRoute route) {
        if (route.consumes() == null) {
            return true;
        }

        String contentType = ctx.contentType();
        if (contentType == null) {
            return false;
        }

        // Compare the media type only: an incoming "application/json; charset=utf-8" satisfies "application/json".
        return mediaType(contentType).equals(mediaType(route.consumes()));
    }

    private static String mediaType(String contentType) {
        int separator = contentType.indexOf(';');
        String value = separator < 0 ? contentType : contentType.substring(0, separator);

        return value.trim().toLowerCase(Locale.ROOT);
    }

    private List<CompiledRoute> compileRoutes(RunContext runContext, String basePath, boolean waitDefault) throws Exception {
        List<CompiledRoute> compiled = new ArrayList<>(routes.size());

        for (RouteDefinition route : routes) {
            String rawMethod = runContext.render(route.getMethod()).as(String.class)
                .orElseThrow(() -> new IllegalArgumentException("Route method is mandatory"));
            String rawPath = runContext.render(route.getPath()).as(String.class)
                .orElseThrow(() -> new IllegalArgumentException("Route path is mandatory"));

            HandlerType method = handlerType(rawMethod);

            compiled.add(new CompiledRoute(
                method,
                normalizePath(basePath, rawPath),
                runContext.render(route.getConsumes()).as(String.class).orElse(null),
                runContext.render(route.getProduces()).as(String.class).orElse("application/json"),
                runContext.render(route.getWait()).as(Boolean.class).orElse(waitDefault),
                runContext.render(route.getBase64Body()).as(Boolean.class).orElse(false)
            ));
        }

        return compiled;
    }

    private static HandlerType handlerType(String method) {
        HandlerType handlerType = ALLOWED_METHODS.get(method.trim().toUpperCase(Locale.ROOT));

        if (handlerType == null) {
            throw new IllegalArgumentException(
                "Unsupported HTTP method '" + method + "', expected one of " + ALLOWED_METHODS.keySet()
            );
        }

        return handlerType;
    }

    static String normalizePath(String base, String path) {
        String b = base.endsWith("/") ? base.substring(0, base.length() - 1) : base;
        String p = path.startsWith("/") ? path : "/" + path;

        return b + p;
    }

    /**
     * Minimal JSON writer for the fixed-shape acknowledgement bodies, so the plugin does not have to bundle or
     * assume a JSON library on the plugin classloader.
     */
    private static String json(Map<String, String> values) {
        return values.entrySet().stream()
            .map(e -> "\"" + escape(e.getKey()) + "\":\"" + escape(e.getValue()) + "\"")
            .collect(Collectors.joining(",", "{", "}"));
    }

    private static String escape(String value) {
        StringBuilder sb = new StringBuilder(value.length() + 8);

        for (char c : value.toCharArray()) {
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }

        return sb.toString();
    }

    private void busyWait() {
        while (isActive.get()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                isActive.set(false);
            }
        }
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void kill() {
        stop(true);
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void stop() {
        stop(false); // must be non-blocking
    }

    private void stop(boolean wait) {
        if (!isActive.compareAndSet(true, false)) {
            return;
        }

        if (wait) {
            try {
                this.waitForTermination.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * A route with every property already rendered and validated.
     */
    private record CompiledRoute(HandlerType method, String fullPath, String consumes, String produces,
                                 boolean synchronous, boolean base64Body) {
    }

    /**
     * Per-request configuration rendered once for the trigger's lifetime.
     */
    private record HandlerConfig(String authHeader, List<String> validKeys, String responseOutput, Duration waitTimeout) {
    }

    /**
     * A fully resolved HTTP response derived from a terminal execution.
     */
    record ResponseSpec(int status, String body, Map<String, String> headers, String contentType) {
    }

    /**
     * Awaits terminal executions by observing the execution queue, and hands each waiting request the execution it
     * started. One subscription is shared for the trigger lifetime; requests register by execution id.
     */
    static final class ExecutionAwaiter implements AutoCloseable {

        private final Map<String, CompletableFuture<Execution>> pending = new ConcurrentHashMap<>();
        private final AtomicReference<Runnable> unsubscribe = new AtomicReference<>();

        @SuppressWarnings({"unchecked", "removal"})
        static ExecutionAwaiter open(RunContext runContext) {
            if (!(runContext instanceof DefaultRunContext defaultRunContext)) {
                throw new IllegalStateException(
                    "Synchronous 'wait' mode requires the standard Kestra runtime; got " + runContext.getClass().getName()
                );
            }

            ApplicationContext applicationContext = defaultRunContext.getApplicationContext();
            QueueInterface<Execution> executionQueue = applicationContext.getBean(
                QueueInterface.class,
                Qualifiers.byName(QueueFactoryInterface.EXECUTION_NAMED)
            );

            ExecutionAwaiter awaiter = new ExecutionAwaiter();
            awaiter.unsubscribe.set(executionQueue.receive(either -> {
                if (either != null && either.isLeft()) {
                    awaiter.onExecution(either.getLeft());
                }
            }));

            return awaiter;
        }

        CompletableFuture<Execution> register(String executionId) {
            CompletableFuture<Execution> completion = new CompletableFuture<>();
            pending.put(executionId, completion);

            return completion;
        }

        void cancel(String executionId) {
            pending.remove(executionId);
        }

        void onExecution(Execution execution) {
            if (execution == null || execution.getState() == null || !execution.getState().isTerminated()) {
                return;
            }

            CompletableFuture<Execution> completion = pending.remove(execution.getId());
            if (completion != null) {
                completion.complete(execution);
            }
        }

        @Override
        public void close() {
            Runnable runnable = unsubscribe.getAndSet(null);
            if (runnable != null) {
                runnable.run();
            }
            pending.values().forEach(completion -> completion.cancel(true));
            pending.clear();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(title = "HTTP method of the request, e.g. `GET`, `POST`")
        private final String method;

        @Schema(title = "Actual request path, e.g. `/api/orders/42`")
        private final String path;

        @Schema(title = "Matched route pattern, e.g. `/api/orders/{id}`")
        private final String matchedRoute;

        @Schema(title = "Path parameters extracted from the URL")
        private final Map<String, String> pathParams;

        @Schema(
            title = "Query parameters from the URL",
            description = "Repeated parameters are joined with a comma."
        )
        private final Map<String, String> queryParams;

        @Schema(title = "Request headers")
        private final Map<String, String> headers;

        @Schema(title = "Raw request body, decoded as a string")
        private final String body;

        @Schema(
            title = "Raw request body, base64-encoded",
            description = "Populated only when the route sets `base64Body: true`, so binary bodies survive intact."
        )
        private final String bodyBase64;

        @Schema(
            title = "Uploaded file parts of a `multipart/form-data` request",
            description = "Each part's `content` is base64-encoded. Empty for non-multipart requests."
        )
        private final List<Part> parts;

        @Schema(title = "Non-file form fields of a `multipart/form-data` request")
        private final Map<String, List<String>> formFields;

        @Schema(title = "`Content-Type` of the request")
        private final String contentType;
    }

    @Builder
    @Getter
    public static class Part {

        @Schema(title = "Form field name of the part")
        private final String name;

        @Schema(title = "Uploaded file name, if the part is a file")
        private final String filename;

        @Schema(title = "`Content-Type` of the part")
        private final String contentType;

        @Schema(title = "Size of the part in bytes")
        private final long size;

        @Schema(title = "Part content, base64-encoded")
        private final String content;
    }
}
