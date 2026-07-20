package io.kestra.plugin.restserver;

import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.HandlerType;
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
import io.kestra.core.runners.RunContext;
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

import java.util.ArrayList;
import java.util.function.Function;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode(callSuper = false)
@Getter
@NoArgsConstructor
@Schema(
    title = "Expose an HTTP API that triggers one execution per request",
    description = """
        Starts an embedded HTTP server for the lifetime of the trigger and registers the declared routes on it,
        in the spirit of Apache Camel's REST DSL. Every matching request creates one realtime execution and is
        answered immediately with `202 Accepted` and the generated execution id — executions are asynchronous,
        so the response does not wait for the flow to finish.

        Requests that match no route get `404`, and requests violating a route's `consumes` get `415`; neither
        creates an execution. The server is bound on the worker that runs the trigger, so the port must be free
        there and reachable by your callers."""
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

        // Rendered once, before the server starts: routes are fixed for the lifetime of the trigger, and a
        // rendering error should fail the trigger rather than an individual request.
        int rPort = runContext.render(this.port).as(Integer.class).orElse(8080);
        String rHost = runContext.render(this.host).as(String.class).orElse("0.0.0.0");
        String rBasePath = runContext.render(this.basePath).as(String.class).orElse("/");
        List<CompiledRoute> compiledRoutes = compileRoutes(runContext, rBasePath);

        return Flux.create(emitter -> {
            AtomicReference<Throwable> error = new AtomicReference<>();

            try {
                Javalin app = Javalin.create(config -> {
                    config.startup.showJavalinBanner = false;
                    config.startup.showOldJavalinVersionWarning = false;

                    for (CompiledRoute route : compiledRoutes) {
                        config.routes.addHttpHandler(
                            route.method(),
                            route.fullPath(),
                            ctx -> handle(ctx, route, conditionContext, triggerContext, emitter)
                        );
                        logger.info("Registering route {} {}", route.method(), route.fullPath());
                    }
                });

                emitter.onDispose(() -> {
                    try {
                        app.stop();
                    } catch (Exception e) {
                        logger.warn("Error while stopping the embedded HTTP server: {}", e.getMessage());
                    } finally {
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
        FluxSink<Execution> emitter
    ) {
        if (!matchesConsumes(ctx, route)) {
            ctx.status(415)
                .contentType("application/json")
                .result(json(Map.of(
                    "status", "rejected",
                    "error", "Unsupported Media Type, expected " + route.consumes()
                )));
            return;
        }

        Output output = Output.builder()
            .method(ctx.method().name())
            .path(ctx.path())
            .matchedRoute(route.fullPath())
            .pathParams(ctx.pathParamMap())
            .queryParams(
                ctx.queryParamMap().entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> String.join(",", e.getValue())))
            )
            .headers(ctx.headerMap())
            .body(ctx.body())
            .contentType(ctx.contentType())
            .build();

        // Built here rather than in a downstream map() so the caller can be told which execution it started.
        Execution execution = TriggerService.generateRealtimeExecution(this, conditionContext, triggerContext, output);
        emitter.next(execution);

        ctx.status(202)
            .contentType(route.produces())
            .result(json(Map.of(
                "status", "accepted",
                "executionId", execution.getId()
            )));
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

    private List<CompiledRoute> compileRoutes(RunContext runContext, String basePath) throws Exception {
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
                runContext.render(route.getProduces()).as(String.class).orElse("application/json")
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
    private record CompiledRoute(HandlerType method, String fullPath, String consumes, String produces) {
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

        @Schema(title = "`Content-Type` of the request")
        private final String contentType;
    }
}
