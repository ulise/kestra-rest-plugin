package io.kestra.plugin.restserver;

import io.javalin.Javalin;
import io.javalin.config.SizeUnit;
import io.javalin.http.Context;
import io.javalin.http.HandlerType;
import io.javalin.http.UploadedFile;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.Label;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.ExecutionTrigger;
import io.kestra.core.models.flows.State;
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
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.ListUtils;
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
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
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
        creates an execution. Callers can be authenticated at the edge with an API key (`apiKey`/`apiKeys`) or
        HTTP Basic (`basicAuth`); a rejected request gets `401` and creates no execution either. The server is
        bound on the worker that runs the trigger, so the port must be free there and reachable by your
        callers."""
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
        ),
        @Example(
            title = "Authenticate legacy callers with HTTP Basic, answering 403 when the credentials are wrong.",
            full = true,
            code = """
                id: basic-auth-api
                namespace: company.myapp

                tasks:
                  - id: handle_request
                    type: io.kestra.plugin.core.log.Log
                    # The password never reaches the flow; the matched username does.
                    message: "Request from {{ trigger.basicAuthUser }}"

                triggers:
                  - id: rest_server
                    type: io.kestra.plugin.restserver.RestServerRealtimeTrigger
                    port: 8090
                    basePath: /api
                    invalidCredentialsStatus: 403
                    authFailureBody: "{}"
                    basicAuth:
                      - username: "{{ secret('PARTNER_A_USER') }}"
                        password: "{{ secret('PARTNER_A_PASSWORD') }}"
                      - username: "{{ secret('PARTNER_B_USER') }}"
                        password: "{{ secret('PARTNER_B_PASSWORD') }}"
                    routes:
                      - method: POST
                        path: /orders
                        consumes: application/json
                """
        ),
        @Example(
            title = "Receive file uploads: multipart parts and a raw binary body both reach the flow as files.",
            full = true,
            code = """
                id: upload-api
                namespace: company.myapp

                tasks:
                  # Both routes hand the flow a kestra:// URI, so no decoding is needed.
                  - id: measure
                    type: io.kestra.plugin.core.storage.Size
                    uri: "{{ trigger.parts[0].uri ?? trigger.uri }}"

                triggers:
                  - id: rest_server
                    type: io.kestra.plugin.restserver.RestServerRealtimeTrigger
                    port: 8090
                    basePath: /api
                    maxRequestSize: 52428800
                    routes:
                      # File parts are always stored, whatever the fetchType.
                      - method: POST
                        path: /feedback
                        produces: application/json
                      # A raw binary body is stored on request.
                      - method: POST
                        path: /images
                        fetchType: STORE
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

    /**
     * Threshold above which an uploaded part is buffered to disk by Jetty rather than kept in the heap.
     */
    private static final long MAX_IN_MEMORY_PART_SIZE = 1024L * 1024;

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
        title = "What to do with the request body",
        description = """
            Default for every route unless a route sets its own `fetchType`. One of:

            - `FETCH` (the default): the body reaches the flow as `{{ trigger.body }}`, decoded as a string.
            - `STORE`: the body is streamed into Kestra's internal storage as it is received, and the flow
              reaches it as `{{ trigger.uri }}`, a `kestra://` URI that any task taking a file accepts. No
              part of it is held in memory and none of it travels through the execution record, so this is
              the mode for large uploads and for binary bodies a string would corrupt.
            - `NONE`: the body is read off the connection and dropped, for a caller whose payload the flow
              does not need.

            File parts of a `multipart/form-data` request are stored whatever this says: a file has no useful
            representation inside an execution. See `{{ trigger.parts }}`."""
    )
    @Builder.Default
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @Schema(
        title = "Wait for the triggered execution and return its result",
        description = """
            Default for every route unless a route sets its own `wait`. When `true`, a request blocks until the
            triggered execution reaches a terminal state, then the response is built from the execution's outputs
            (see `responseOutput`). When `false` (the default), requests are answered immediately with
            `202 Accepted` and never wait.

            Synchronous mode requires the trigger's worker and the executor to share a JVM (`server local`,
            `server standalone`, or a single-replica deployment). It is queue-backend agnostic and works on
            the memory, H2, MySQL and Postgres queues alike, but on JDBC backends the queue is polled rather
            than dispatched in-process, adding up to `kestra.jdbc.queues.max-poll-interval` (default 500ms)
            of latency on an otherwise idle instance. It is not supported when the trigger's worker is a
            separate process from the executor: only that worker binds the port."""
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

    @Schema(
        title = "Accepted HTTP Basic credentials",
        description = """
            For callers that authenticate with `Authorization: Basic …` and cannot be asked to switch to an API
            key. When set (non-empty), a request is authorized if its credentials match any entry; list several
            pairs to front multiple callers. Combined with `apiKey`/`apiKeys` — a request passes if it satisfies
            *either* scheme — and, like them, rejection happens before any route matching or execution.

            The scheme token is matched case-insensitively (`Basic`, `basic`), as required by RFC 9110. Unlike
            the API key, the credentials do **not** reach the flow: `Authorization` is stripped from
            `{{ trigger.headers }}` and the matched username is exposed as `{{ trigger.basicAuthUser }}`
            instead, so the caller's password is never written to the execution."""
    )
    @PluginProperty(group = "connection")
    private List<BasicCredential> basicAuth;

    @Schema(
        title = "Status returned when credentials are present but wrong",
        description = """
            Applies when a request carries credentials that parse but do not match — a well-formed
            `Authorization: Basic …` header, or a present but non-matching API key. Requests with no credentials
            at all, or with an `Authorization` header that is absent, not `Basic`, or undecodable, always get
            `401`, because nothing could be compared.

            Defaults to `401`, which is what RFC 9110 prescribes. Set it to `403` only to preserve an existing
            caller contract that distinguishes the two: it is non-standard, and it tells a caller that its header
            was well-formed."""
    )
    @Builder.Default
    private Property<Integer> invalidCredentialsStatus = Property.ofValue(401);

    @Schema(
        title = "Body returned when a request is rejected by authentication",
        description = """
            Returned verbatim for both the `401` and the invalid-credentials response, with content type
            `application/json`. Use it to match an existing caller contract, e.g. `{}`. When null, the plugin's
            own bodies are used: `{"status":"UNAUTHORIZED"}` and `{"status":"FORBIDDEN"}`."""
    )
    private Property<String> authFailureBody;

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
        FetchType rFetchTypeDefault = runContext.render(this.fetchType).as(FetchType.class).orElse(FetchType.FETCH);
        List<CompiledRoute> compiledRoutes = compileRoutes(runContext, rBasePath, rWaitDefault, rFetchTypeDefault);

        // Resolved before the server starts: every route can receive an upload, and an instance that cannot store
        // one should fail the trigger rather than a caller's request halfway through.
        RequestStorage storage = RequestStorage.of(runContext, triggerContext);

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

        // Each accepted pair is reduced to a digest of "user:password", the exact byte sequence a Basic header
        // decodes to, so a request is checked with one constant-time comparison per credential.
        List<byte[]> basicDigests = new ArrayList<>();
        for (BasicCredential credential : ListUtils.emptyOnNull(this.basicAuth)) {
            String user = runContext.render(credential.getUsername()).as(String.class).orElse(null);
            String password = runContext.render(credential.getPassword()).as(String.class).orElse(null);

            if (user == null || user.isEmpty() || password == null) {
                throw new IllegalArgumentException("A 'basicAuth' entry needs a non-empty username and a password");
            }

            basicDigests.add(sha256(user + ":" + password));
        }

        HandlerConfig config = new HandlerConfig(
            runContext.render(this.authHeader).as(String.class).orElse("X-Api-Key"),
            List.copyOf(validKeys),
            List.copyOf(basicDigests),
            runContext.render(this.invalidCredentialsStatus).as(Integer.class).orElse(401),
            runContext.render(this.authFailureBody).as(String.class).orElse(null),
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
                    // Parts above this spill to a temporary file instead of the heap. Kept small deliberately: a
                    // part is streamed straight into the internal storage, so holding it in memory first would
                    // undo the point of storing it.
                    config2.jetty.multipartConfig.maxInMemoryFileSize(
                        (int) Math.min(rMaxRequestSize, MAX_IN_MEMORY_PART_SIZE), SizeUnit.BYTES);

                    for (CompiledRoute route : compiledRoutes) {
                        config2.routes.addHttpHandler(
                            route.method(),
                            route.fullPath(),
                            ctx -> handle(ctx, route, conditionContext, triggerContext, emitter, config, awaiter, storage)
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
        ExecutionAwaiter awaiter,
        RequestStorage storage
    ) {
        // Authentication runs before route logic, so an unauthenticated caller learns nothing about the routes.
        BasicCredentials basic = config.basicDigests().isEmpty()
            ? null
            : parseBasic(header(ctx.headerMap(), "Authorization"));
        AuthResult auth = authenticate(ctx.headerMap(), basic, config);

        if (auth != AuthResult.AUTHORIZED) {
            int status = auth == AuthResult.INVALID ? config.invalidCredentialsStatus() : 401;
            if (status == 401 && !config.basicDigests().isEmpty()) {
                // RFC 9110: a 401 must say how to authenticate. Only sent when Basic is configured, so an
                // API-key-only server keeps answering exactly as before.
                ctx.header("WWW-Authenticate", "Basic");
            }

            ctx.status(status)
                .contentType("application/json")
                .result(config.authFailureBody() != null
                    ? config.authFailureBody()
                    : json(Map.of("status", auth == AuthResult.INVALID ? "FORBIDDEN" : "UNAUTHORIZED")));
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
            .headers(config.basicDigests().isEmpty() ? ctx.headerMap() : withoutAuthorization(ctx.headerMap()))
            .basicAuthUser(basic == null ? null : basic.username())
            .contentType(ctx.contentType());

        // Minted before a byte of the body is read, so that whatever the request carries is stored under the
        // execution it is about to create, and is purged with it.
        String executionId = IdUtils.create();
        boolean anythingStored = false;
        Execution execution;

        try {
            if (ctx.isMultipartFormData()) {
                // A file has no useful representation inside an execution, so parts are stored whatever the
                // route's fetchType says, and the flow reaches them by URI.
                List<Part> parts = storeParts(ctx, storage, executionId);
                anythingStored = !parts.isEmpty();
                builder.parts(parts).formFields(ctx.formParamMap());
            } else {
                switch (route.fetchType()) {
                    case FETCH -> {
                        builder.body(ctx.body());
                        if (route.base64Body()) {
                            builder.bodyBase64(Base64.getEncoder().encodeToString(ctx.bodyAsBytes()));
                        }
                    }
                    case STORE -> {
                        URI uri = storage.storeBody(executionId, ctx.bodyInputStream());
                        anythingStored = uri != null;
                        builder.uri(uri == null ? null : uri.toString());
                    }
                    // Read and dropped rather than left unclaimed, so the response is not sent over a
                    // connection cut short mid-upload.
                    case NONE -> drain(ctx);
                }
            }

            Output output = builder.build();

            // Built here rather than in a downstream map() so the caller can be told which execution it started.
            execution = generateExecution(executionId, output, conditionContext, triggerContext);
        } catch (Exception e) {
            // Nothing will ever purge what was stored, since the execution it is scoped to will not exist.
            if (anythingStored) {
                storage.deleteStored(executionId);
            }

            ctx.status(500)
                .contentType("application/json")
                .result(json(Map.of("status", "error", "error", e.getMessage() != null ? e.getMessage() : e.toString())));
            return;
        }

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
     * Outcome of the authentication gate. {@code MISSING} and {@code INVALID} are distinguished so a caller
     * contract that answers {@code 401} for "no usable credentials" and something else for "wrong credentials"
     * can be preserved; see {@code invalidCredentialsStatus}.
     */
    enum AuthResult {
        AUTHORIZED,
        MISSING,
        INVALID
    }

    /**
     * Runs every configured scheme and combines them with OR: a request passes if it satisfies the API key
     * <em>or</em> Basic auth, matching how {@code apiKey} and {@code apiKeys} already combine. When none is
     * configured the gate is open. A failure reports {@code INVALID} only if some scheme actually had
     * credentials to compare, so an empty-handed request never sees the invalid-credentials status.
     */
    static AuthResult authenticate(Map<String, String> headers, BasicCredentials basic, HandlerConfig config) {
        boolean apiKeyConfigured = !config.validKeys().isEmpty();
        boolean basicConfigured = !config.basicDigests().isEmpty();

        if (!apiKeyConfigured && !basicConfigured) {
            return AuthResult.AUTHORIZED;
        }

        String providedKey = apiKeyConfigured ? header(headers, config.authHeader()) : null;

        if (apiKeyConfigured && providedKey != null && matchesAny(providedKey, config.validKeys())) {
            return AuthResult.AUTHORIZED;
        }

        if (basicConfigured && basic != null && matchesAnyDigest(basic.username() + ":" + basic.password(), config.basicDigests())) {
            return AuthResult.AUTHORIZED;
        }

        // Something was presented and compared, so the caller is wrong rather than merely absent. A malformed
        // or non-Basic Authorization header leaves `basic` null and therefore counts as missing, not invalid:
        // nothing could be compared against it.
        boolean presented = (apiKeyConfigured && providedKey != null) || (basicConfigured && basic != null);

        return presented ? AuthResult.INVALID : AuthResult.MISSING;
    }

    /**
     * Case-insensitive header lookup. HTTP header names are case-insensitive and gateways normalise them, so an
     * exact-case {@code get} could silently find nothing and read as "no credentials required" — a fail-open bug
     * this deliberately avoids.
     */
    static String header(Map<String, String> headers, String name) {
        for (Map.Entry<String, String> header : headers.entrySet()) {
            if (header.getKey().equalsIgnoreCase(name)) {
                return header.getValue();
            }
        }

        return null;
    }

    /**
     * Parses an {@code Authorization: Basic <base64>} header, returning null when it is absent, uses another
     * scheme, or cannot be decoded. The scheme token is compared case-insensitively as RFC 9110 requires —
     * Javalin's own {@code basicAuthCredentials()} matches {@code "Basic "} exactly, which would make a caller
     * sending {@code basic} look like it presented nothing at all.
     * <p>
     * The credentials are decoded as UTF-8 per RFC 7617. A legacy client that encodes a non-ASCII password as
     * ISO-8859-1 will therefore not match; ASCII credentials, the overwhelmingly common case, are unaffected.
     */
    static BasicCredentials parseBasic(String headerValue) {
        if (headerValue == null) {
            return null;
        }

        String value = headerValue.strip();
        int space = value.indexOf(' ');
        if (space < 0 || !value.substring(0, space).equalsIgnoreCase("Basic")) {
            return null;
        }

        byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(value.substring(space + 1).strip());
        } catch (IllegalArgumentException e) {
            return null;
        }

        String credentials = new String(decoded, StandardCharsets.UTF_8);
        int colon = credentials.indexOf(':');
        if (colon < 0) {
            return null;
        }

        // Only the first colon separates: RFC 7617 forbids one in the username but allows it in the password.
        return new BasicCredentials(credentials.substring(0, colon), credentials.substring(colon + 1));
    }

    /**
     * Returns the headers without {@code Authorization}, so Basic credentials never reach the flow. They would
     * otherwise be persisted in the execution's trigger variables — and base64 is not encryption, so the
     * caller's password would be readable by anyone with execution read access.
     */
    static Map<String, String> withoutAuthorization(Map<String, String> headers) {
        Map<String, String> filtered = new LinkedHashMap<>(headers);
        filtered.keySet().removeIf(name -> name.equalsIgnoreCase("Authorization"));

        return filtered;
    }

    /**
     * Constant-time membership test. Every candidate is compared (no early exit) so the number of comparisons
     * does not depend on which one matched, and both sides are hashed first so a mismatch in length is not
     * distinguishable from a mismatch in content.
     */
    private static boolean matchesAny(String provided, Collection<String> expected) {
        byte[] digest = sha256(provided);
        boolean match = false;
        for (String candidate : expected) {
            match |= MessageDigest.isEqual(digest, sha256(candidate));
        }

        return match;
    }

    private static boolean matchesAnyDigest(String provided, Collection<byte[]> expectedDigests) {
        byte[] digest = sha256(provided);
        boolean match = false;
        for (byte[] candidate : expectedDigests) {
            match |= MessageDigest.isEqual(digest, candidate);
        }

        return match;
    }

    private static byte[] sha256(String value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandated by the JDK, so this cannot happen on a valid runtime.
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    /**
     * Username and password decoded from a Basic {@code Authorization} header.
     */
    record BasicCredentials(String username, String password) {
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
     * Streams every uploaded file part into the internal storage, and describes it as a {@link Part} carrying the
     * URI it was stored under. The bytes never reach the execution, which is both what keeps a large upload out of
     * the execution record and what lets a flow hand the part to any task that takes a file.
     * <p>
     * Parts are numbered by their position in the request: their filenames are chosen by the caller, and two parts
     * of one request may well carry the same one.
     */
    private static List<Part> storeParts(Context ctx, RequestStorage storage, String executionId) throws IOException {
        List<Part> parts = new ArrayList<>();
        int index = 0;

        for (Map.Entry<String, List<UploadedFile>> uploaded : ctx.uploadedFileMap().entrySet()) {
            for (UploadedFile file : uploaded.getValue()) {
                URI uri;
                try (InputStream content = file.content()) {
                    uri = storage.storePart(executionId, index++, file.filename(), content);
                }

                parts.add(Part.builder()
                    .name(uploaded.getKey())
                    .filename(file.filename())
                    .contentType(file.contentType())
                    .size(file.size())
                    .uri(uri.toString())
                    .build());
            }
        }

        return parts;
    }

    /**
     * Reads the body off the connection and discards it, for a route that does not want it.
     */
    private static void drain(Context ctx) throws IOException {
        try (InputStream body = ctx.bodyInputStream()) {
            body.transferTo(OutputStream.nullOutputStream());
        }
    }

    /**
     * Builds the execution a request starts, with an id chosen by the caller of this method.
     * <p>
     * This mirrors {@link TriggerService#generateRealtimeExecution}, which cannot be used here because it mints the
     * id itself: the files a request brings with it have to be stored before the output describing them exists, and
     * they are stored under the execution id, so that id has to be known first.
     */
    private Execution generateExecution(
        String id,
        Output output,
        ConditionContext conditionContext,
        TriggerContext triggerContext
    ) {
        ExecutionTrigger executionTrigger = ExecutionTrigger.of(this, output, conditionContext.getRunContext().logFileURI());

        List<Label> labels = new ArrayList<>(ListUtils.emptyOnNull(this.getLabels()));
        labels.add(new Label(Label.FROM, "trigger"));
        if (labels.stream().noneMatch(label -> Label.CORRELATION_ID.equals(label.key()))) {
            labels.add(new Label(Label.CORRELATION_ID, id));
        }

        return Execution.builder()
            .id(id)
            .namespace(triggerContext.getNamespace())
            .flowId(triggerContext.getFlowId())
            .tenantId(triggerContext.getTenantId())
            .flowRevision(conditionContext.getFlow().getRevision())
            .variables(conditionContext.getFlow().getVariables())
            .state(new State())
            .trigger(executionTrigger)
            .labels(labels)
            .build();
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

    private List<CompiledRoute> compileRoutes(
        RunContext runContext,
        String basePath,
        boolean waitDefault,
        FetchType fetchTypeDefault
    ) throws Exception {
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
                runContext.render(route.getFetchType()).as(FetchType.class).orElse(fetchTypeDefault),
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
                                 boolean synchronous, FetchType fetchType, boolean base64Body) {
    }

    /**
     * Per-request configuration rendered once for the trigger's lifetime.
     */
    record HandlerConfig(
        String authHeader,
        List<String> validKeys,
        List<byte[]> basicDigests,
        int invalidCredentialsStatus,
        String authFailureBody,
        String responseOutput,
        Duration waitTimeout
    ) {
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

        @Schema(
            title = "Request headers",
            description = "`Authorization` is omitted when `basicAuth` is configured, so credentials are not persisted."
        )
        private final Map<String, String> headers;

        @Schema(
            title = "Username from the request's Basic credentials",
            description = """
                Populated only when `basicAuth` is configured and the request carried a parseable
                `Authorization: Basic …` header. Use it to map the caller to their data, in place of the
                `Authorization` header that is stripped from `headers`."""
        )
        private final String basicAuthUser;

        @Schema(
            title = "Raw request body, decoded as a string",
            description = "Populated only for a route whose `fetchType` is `FETCH`, and never for a `multipart/form-data` request, whose parts are available as `parts` and `formFields`."
        )
        private final String body;

        @Schema(
            title = "Raw request body, base64-encoded",
            description = "Populated only when the route sets the deprecated `base64Body: true` and fetches the body, so binary bodies survive intact. Prefer `fetchType: STORE` and `uri`."
        )
        private final String bodyBase64;

        @Schema(
            title = "URI of the request body in Kestra's internal storage",
            description = """
                Populated only for a route whose `fetchType` is `STORE`, and only when the request carried a
                body. The body is streamed to the storage as it is received, so a payload of any size reaches
                the flow intact without travelling through the execution. Hand it to any task that takes a
                `kestra://` file. It is stored under the execution the request creates, and purged with it."""
        )
        private final String uri;

        @Schema(
            title = "Uploaded file parts of a `multipart/form-data` request",
            description = "Each part's content is stored in Kestra's internal storage and reached by its `uri`. Empty for non-multipart requests."
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

        @Schema(
            title = "URI of the part's content in Kestra's internal storage",
            description = """
                The part is streamed to the storage as it is received, so its bytes reach the flow intact and
                without travelling through the execution. Hand it to any task that takes a `kestra://` file. It
                is stored under the execution the request creates, and purged with it."""
        )
        private final String uri;
    }
}
