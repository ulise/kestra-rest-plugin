package io.kestra.plugin.restserver;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.Trigger;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class RestServerRealtimeTriggerTest {

    private static final HttpClient CLIENT = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(2))
        .build();

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    private QueueInterface<Execution> executionQueue;

    @Test
    void postFiresExecutionWithBody() throws Exception {
        int port = freePort();
        RestServerRealtimeTrigger trigger = trigger(port, "/api", route("POST", "/orders", "application/json", null));

        withRunningServer(trigger, port, executions -> {
            HttpResponse<String> response = send(
                request(port, "/api/orders").header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("{\"item\":\"widget\"}"))
            );

            assertThat(response.statusCode(), is(202));
            assertThat(response.headers().firstValue("Content-Type").orElse(""), containsString("application/json"));

            await().atMost(Duration.ofSeconds(5)).until(() -> executions.size() == 1);

            RestServerRealtimeTrigger.Output output = output(executions.getFirst());
            assertThat(output.getMethod(), is("POST"));
            assertThat(output.getPath(), is("/api/orders"));
            assertThat(output.getMatchedRoute(), is("/api/orders"));
            assertThat(output.getBody(), is("{\"item\":\"widget\"}"));
            assertThat(output.getContentType(), is("application/json"));

            // The caller is told which execution its request started.
            assertThat(response.body(), allOf(
                containsString("\"status\":\"accepted\""),
                containsString("\"executionId\":\"" + executions.getFirst().getId() + "\"")
            ));
        });
    }

    @Test
    void getPopulatesPathAndQueryParams() throws Exception {
        int port = freePort();
        RestServerRealtimeTrigger trigger = trigger(port, "/api", route("GET", "/orders/{id}", null, null));

        withRunningServer(trigger, port, executions -> {
            HttpResponse<String> response = send(request(port, "/api/orders/42?status=open&tag=a&tag=b").GET());

            assertThat(response.statusCode(), is(202));
            await().atMost(Duration.ofSeconds(5)).until(() -> executions.size() == 1);

            RestServerRealtimeTrigger.Output output = output(executions.getFirst());
            assertThat(output.getPath(), is("/api/orders/42"));
            assertThat(output.getMatchedRoute(), is("/api/orders/{id}"));
            assertThat(output.getPathParams(), hasEntry("id", "42"));
            assertThat(output.getQueryParams(), hasEntry("status", "open"));
            // Repeated query parameters are joined rather than dropped.
            assertThat(output.getQueryParams(), hasEntry("tag", "a,b"));
        });
    }

    @Test
    void unmatchedRouteReturns404WithoutExecution() throws Exception {
        int port = freePort();
        RestServerRealtimeTrigger trigger = trigger(port, "/api", route("GET", "/orders", null, null));

        withRunningServer(trigger, port, executions -> {
            assertThat(send(request(port, "/api/unknown").GET()).statusCode(), is(404));
            // A wrong method on a declared path is equally unmatched.
            assertThat(send(request(port, "/api/orders").DELETE()).statusCode(), is(404));

            assertThat(executions, is(empty()));
        });
    }

    @Test
    void mismatchedContentTypeReturns415WithoutExecution() throws Exception {
        int port = freePort();
        RestServerRealtimeTrigger trigger = trigger(port, "/api", route("POST", "/orders", "application/json", null));

        withRunningServer(trigger, port, executions -> {
            HttpResponse<String> rejected = send(
                request(port, "/api/orders").header("Content-Type", "text/plain")
                    .POST(HttpRequest.BodyPublishers.ofString("nope"))
            );
            assertThat(rejected.statusCode(), is(415));
            assertThat(executions, is(empty()));

            // A charset parameter must not defeat the match.
            HttpResponse<String> accepted = send(
                request(port, "/api/orders").header("Content-Type", "application/json; charset=utf-8")
                    .POST(HttpRequest.BodyPublishers.ofString("{}"))
            );
            assertThat(accepted.statusCode(), is(202));
            await().atMost(Duration.ofSeconds(5)).until(() -> executions.size() == 1);
        });
    }

    @Test
    void multipleRoutesAreServedAndBasePathDefaultsToRoot() throws Exception {
        int port = freePort();
        RestServerRealtimeTrigger trigger = RestServerRealtimeTrigger.builder()
            .id("rest_server")
            .type(RestServerRealtimeTrigger.class.getName())
            .port(Property.ofValue(port))
            .routes(List.of(
                route("GET", "/orders/{id}", null, null),
                route("DELETE", "/orders/{id}", null, "text/plain")
            ))
            .build();

        withRunningServer(trigger, port, executions -> {
            assertThat(send(request(port, "/orders/1").GET()).statusCode(), is(202));

            HttpResponse<String> deleted = send(request(port, "/orders/1").DELETE());
            assertThat(deleted.statusCode(), is(202));
            assertThat(deleted.headers().firstValue("Content-Type").orElse(""), containsString("text/plain"));

            await().atMost(Duration.ofSeconds(5)).until(() -> executions.size() == 2);
            assertThat(output(executions.get(1)).getMatchedRoute(), is("/orders/{id}"));
        });
    }

    @Test
    void serverStopsWhenTriggerStops() throws Exception {
        int port = freePort();
        RestServerRealtimeTrigger trigger = trigger(port, "/api", route("GET", "/orders", null, null));

        List<Execution> executions = new CopyOnWriteArrayList<>();
        Disposable subscription = subscribe(trigger, executions);
        awaitListening(port);

        trigger.stop();
        subscription.dispose();

        // The port must be released, so a Kestra restart can rebind it.
        await().atMost(Duration.ofSeconds(10)).until(() -> {
            try (ServerSocket ignored = new ServerSocket(port)) {
                return true;
            } catch (IOException e) {
                return false;
            }
        });

        assertThrows(ConnectException.class, () -> CLIENT.send(
            request(port, "/api/orders").GET().build(),
            HttpResponse.BodyHandlers.ofString()
        ));
    }

    @Test
    void unsupportedMethodIsRejectedBeforeTheServerStarts() throws Exception {
        int port = freePort();
        RestServerRealtimeTrigger trigger = trigger(port, "/api", route("BEFORE", "/orders", null, null));

        Map.Entry<ConditionContext, Trigger> mock = TestsUtils.mockTrigger(runContextFactory, trigger);

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> trigger.evaluate(mock.getKey(), mock.getValue())
        );
        assertThat(exception.getMessage(), containsString("Unsupported HTTP method"));
    }

    @Test
    void normalizePathJoinsBaseAndRoute() {
        assertThat(RestServerRealtimeTrigger.normalizePath("/", "/orders"), is("/orders"));
        assertThat(RestServerRealtimeTrigger.normalizePath("/api", "/orders"), is("/api/orders"));
        assertThat(RestServerRealtimeTrigger.normalizePath("/api/", "orders"), is("/api/orders"));
        assertThat(RestServerRealtimeTrigger.normalizePath("/api", "orders/{id}"), is("/api/orders/{id}"));
    }

    // -------------------------------------------------------------------------------------------------------------
    // #3 API-key authentication
    // -------------------------------------------------------------------------------------------------------------

    @Test
    void authorizedIsCaseInsensitiveOnHeaderNameAndFailsClosed() {
        assertThat(RestServerRealtimeTrigger.authorized(Map.of("X-Api-Key", "s3cret"), "X-Api-Key", List.of("s3cret")), is(true));
        // Header names are case-insensitive: a normalised "x-api-key" must still match.
        assertThat(RestServerRealtimeTrigger.authorized(Map.of("x-api-key", "s3cret"), "X-Api-Key", List.of("s3cret")), is(true));
        // Wrong value is rejected.
        assertThat(RestServerRealtimeTrigger.authorized(Map.of("X-Api-Key", "nope"), "X-Api-Key", List.of("s3cret")), is(false));
        // Missing header fails closed, never "no key required".
        assertThat(RestServerRealtimeTrigger.authorized(Map.of(), "X-Api-Key", List.of("s3cret")), is(false));
    }

    @Test
    void authorizedAcceptsAnyKeyInTheList() {
        List<String> keys = List.of("key-aaa", "key-bbb", "key-ccc");
        // Any listed key is accepted.
        assertThat(RestServerRealtimeTrigger.authorized(Map.of("X-Api-Key", "key-aaa"), "X-Api-Key", keys), is(true));
        assertThat(RestServerRealtimeTrigger.authorized(Map.of("X-Api-Key", "key-ccc"), "X-Api-Key", keys), is(true));
        // A key not in the list is rejected.
        assertThat(RestServerRealtimeTrigger.authorized(Map.of("X-Api-Key", "key-zzz"), "X-Api-Key", keys), is(false));
    }

    @Test
    void apiKeyGuardsRequestsBeforeAnyExecution() throws Exception {
        int port = freePort();
        RestServerRealtimeTrigger trigger = RestServerRealtimeTrigger.builder()
            .id("rest_server")
            .type(RestServerRealtimeTrigger.class.getName())
            .port(Property.ofValue(port))
            .basePath(Property.ofValue("/api"))
            .apiKey(Property.ofValue("s3cret"))
            .routes(List.of(route("GET", "/orders", null, null)))
            .build();

        withRunningServer(trigger, port, executions -> {
            assertThat(send(request(port, "/api/orders").GET()).statusCode(), is(401));
            assertThat(send(request(port, "/api/orders").header("X-Api-Key", "nope").GET()).statusCode(), is(401));
            // Neither rejected request started an execution.
            assertThat(executions, is(empty()));

            // Correct key, supplied under a lower-cased header name, is accepted.
            assertThat(send(request(port, "/api/orders").header("x-api-key", "s3cret").GET()).statusCode(), is(202));
            await().atMost(Duration.ofSeconds(5)).until(() -> executions.size() == 1);
        });
    }

    @Test
    void apiKeysListAcceptsAnyPartnerKeyAndForwardsItToTheFlow() throws Exception {
        int port = freePort();
        RestServerRealtimeTrigger trigger = RestServerRealtimeTrigger.builder()
            .id("rest_server")
            .type(RestServerRealtimeTrigger.class.getName())
            .port(Property.ofValue(port))
            .basePath(Property.ofValue("/api"))
            .apiKeys(Property.ofValue(List.of("key-aaa", "key-bbb")))
            .routes(List.of(route("GET", "/orders", null, null)))
            .build();

        withRunningServer(trigger, port, executions -> {
            // Two different partner keys are both accepted.
            assertThat(send(request(port, "/api/orders").header("X-Api-Key", "key-aaa").GET()).statusCode(), is(202));
            assertThat(send(request(port, "/api/orders").header("X-Api-Key", "key-bbb").GET()).statusCode(), is(202));
            // A key outside the list is rejected.
            assertThat(send(request(port, "/api/orders").header("X-Api-Key", "key-zzz").GET()).statusCode(), is(401));

            await().atMost(Duration.ofSeconds(5)).until(() -> executions.size() == 2);
            // The matched key reaches the flow so it can map the caller to partner data.
            String forwarded = output(executions.getFirst()).getHeaders().entrySet().stream()
                .filter(e -> e.getKey().equalsIgnoreCase("X-Api-Key"))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
            assertThat(forwarded, is("key-aaa"));
        });
    }

    // -------------------------------------------------------------------------------------------------------------
    // #2 Flow-controlled response mapping
    // -------------------------------------------------------------------------------------------------------------

    @Test
    void mapResponseUsesFlowOutputForStatusBodyAndHeaders() throws Exception {
        Execution execution = Execution.builder()
            .id("e1")
            .state(new State(State.Type.SUCCESS))
            .outputs(Map.of("response", Map.of(
                "status", 404,
                "body", "{\"status\":\"NOT_FOUND\"}",
                "headers", Map.of("X-Trace-Id", "t1")
            )))
            .build();

        RestServerRealtimeTrigger.ResponseSpec spec =
            RestServerRealtimeTrigger.mapResponse(execution, "response", "application/json");

        // Non-2xx status with a body that survives verbatim — the crux of issue #2.
        assertThat(spec.status(), is(404));
        assertThat(spec.body(), is("{\"status\":\"NOT_FOUND\"}"));
        assertThat(spec.headers(), hasEntry("X-Trace-Id", "t1"));
    }

    @Test
    void mapResponseSerialisesObjectBodyAndDefaultsStatusByState() throws Exception {
        // Object body is JSON-serialised; missing status defaults to 200 on success.
        Execution success = Execution.builder()
            .id("e2")
            .state(new State(State.Type.SUCCESS))
            .outputs(Map.of("response", Map.of("body", Map.of("hello", "world"))))
            .build();
        RestServerRealtimeTrigger.ResponseSpec successSpec =
            RestServerRealtimeTrigger.mapResponse(success, "response", "application/json");
        assertThat(successSpec.status(), is(200));
        assertThat(successSpec.body(), containsString("\"hello\":\"world\""));

        // No response output on a failed execution defaults to 500.
        Execution failed = Execution.builder()
            .id("e3")
            .state(new State(State.Type.FAILED))
            .outputs(Map.of())
            .build();
        RestServerRealtimeTrigger.ResponseSpec failedSpec =
            RestServerRealtimeTrigger.mapResponse(failed, "response", "application/json");
        assertThat(failedSpec.status(), is(500));
    }

    // -------------------------------------------------------------------------------------------------------------
    // #1 Synchronous wait mode
    // -------------------------------------------------------------------------------------------------------------

    @Test
    void awaiterCompletesOnlyOnTerminalMatchingExecution() throws Exception {
        RestServerRealtimeTrigger.ExecutionAwaiter awaiter = new RestServerRealtimeTrigger.ExecutionAwaiter();
        CompletableFuture<Execution> pending = awaiter.register("exec-1");

        // A non-terminal state for the same id must not complete the request.
        awaiter.onExecution(Execution.builder().id("exec-1").state(new State(State.Type.RUNNING)).build());
        assertThat(pending.isDone(), is(false));

        // A terminal state for a different id must not complete it either.
        awaiter.onExecution(Execution.builder().id("other").state(new State(State.Type.SUCCESS)).build());
        assertThat(pending.isDone(), is(false));

        // Terminal + matching id completes with that execution.
        awaiter.onExecution(Execution.builder().id("exec-1").state(new State(State.Type.SUCCESS)).build());
        assertThat(pending.isDone(), is(true));
        assertThat(pending.get().getId(), is("exec-1"));
    }

    @Test
    void syncModeReturnsFlowControlledResponse() throws Exception {
        int port = freePort();
        RestServerRealtimeTrigger trigger = RestServerRealtimeTrigger.builder()
            .id("rest_server")
            .type(RestServerRealtimeTrigger.class.getName())
            .port(Property.ofValue(port))
            .basePath(Property.ofValue("/api"))
            .wait(Property.ofValue(true))
            .waitTimeout(Property.ofValue(Duration.ofSeconds(15)))
            .routes(List.of(route("GET", "/orders/{id}", null, null)))
            .build();

        List<Execution> executions = new CopyOnWriteArrayList<>();
        Disposable subscription = subscribe(trigger, executions);

        try {
            awaitListening(port);

            // The request blocks until we push a terminal execution, so it runs off the test thread.
            CompletableFuture<HttpResponse<String>> pending = CompletableFuture.supplyAsync(() ->
                send(request(port, "/api/orders/42").timeout(Duration.ofSeconds(20)).GET()));

            // Wait until the trigger has started (and registered) the execution, then complete it out of band.
            await().atMost(Duration.ofSeconds(5)).until(() -> executions.size() == 1);
            Execution terminal = executions.getFirst()
                .withState(State.Type.SUCCESS)
                .withOutputs(Map.of("response", Map.of(
                    "status", 404,
                    "body", "{\"status\":\"NOT_FOUND\"}",
                    "headers", Map.of("X-Trace-Id", "abc")
                )));
            executionQueue.emit(terminal);

            HttpResponse<String> response = pending.get(20, TimeUnit.SECONDS);
            assertThat(response.statusCode(), is(404));
            assertThat(response.body(), is("{\"status\":\"NOT_FOUND\"}"));
            assertThat(response.headers().firstValue("X-Trace-Id").orElse(""), is("abc"));
        } finally {
            trigger.stop();
            subscription.dispose();
        }
    }

    @Test
    void syncModeTimesOutWhenExecutionNeverCompletes() throws Exception {
        int port = freePort();
        RestServerRealtimeTrigger trigger = RestServerRealtimeTrigger.builder()
            .id("rest_server")
            .type(RestServerRealtimeTrigger.class.getName())
            .port(Property.ofValue(port))
            .basePath(Property.ofValue("/api"))
            .wait(Property.ofValue(true))
            .waitTimeout(Property.ofValue(Duration.ofSeconds(1)))
            .routes(List.of(route("GET", "/orders/{id}", null, null)))
            .build();

        withRunningServer(trigger, port, executions -> {
            HttpResponse<String> response = send(request(port, "/api/orders/42").timeout(Duration.ofSeconds(10)).GET());
            assertThat(response.statusCode(), is(504));
            assertThat(response.body(), containsString("timeout"));
            // The execution was still started; only the response gave up waiting.
            await().atMost(Duration.ofSeconds(5)).until(() -> executions.size() == 1);
        });
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Runs {@code assertions} against a started server, and tears the server down afterwards.
     */
    private void withRunningServer(RestServerRealtimeTrigger trigger, int port, ServerAssertions assertions) throws Exception {
        List<Execution> executions = new CopyOnWriteArrayList<>();
        Disposable subscription = subscribe(trigger, executions);

        try {
            awaitListening(port);
            assertions.run(executions);
        } finally {
            trigger.stop();
            subscription.dispose();
        }
    }

    /**
     * {@code evaluate()} blocks its thread for the lifetime of the server, so it has to run off the test thread.
     */
    private Disposable subscribe(RestServerRealtimeTrigger trigger, List<Execution> executions) throws Exception {
        Map.Entry<ConditionContext, Trigger> mock = TestsUtils.mockTrigger(runContextFactory, trigger);

        return Flux.from(trigger.evaluate(mock.getKey(), mock.getValue()))
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(executions::add);
    }

    private static void awaitListening(int port) {
        await().atMost(Duration.ofSeconds(10)).until(() -> {
            try (ServerSocket ignored = new ServerSocket(port)) {
                return false; // still bindable, so the server has not started yet
            } catch (IOException e) {
                return true;
            }
        });
    }

    @SuppressWarnings("unchecked")
    private static RestServerRealtimeTrigger.Output output(Execution execution) {
        Map<String, Object> variables = (Map<String, Object>) execution.getTrigger().getVariables();

        return RestServerRealtimeTrigger.Output.builder()
            .method((String) variables.get("method"))
            .path((String) variables.get("path"))
            .matchedRoute((String) variables.get("matchedRoute"))
            .pathParams((Map<String, String>) variables.get("pathParams"))
            .queryParams((Map<String, String>) variables.get("queryParams"))
            .headers((Map<String, String>) variables.get("headers"))
            .body((String) variables.get("body"))
            .contentType((String) variables.get("contentType"))
            .build();
    }

    private static RestServerRealtimeTrigger trigger(int port, String basePath, RouteDefinition... routes) {
        return RestServerRealtimeTrigger.builder()
            .id("rest_server")
            .type(RestServerRealtimeTrigger.class.getName())
            .port(Property.ofValue(port))
            .basePath(Property.ofValue(basePath))
            .routes(List.of(routes))
            .build();
    }

    private static RouteDefinition route(String method, String path, String consumes, String produces) {
        return RouteDefinition.builder()
            .method(Property.ofValue(method))
            .path(Property.ofValue(path))
            .consumes(consumes == null ? null : Property.ofValue(consumes))
            .produces(produces == null ? null : Property.ofValue(produces))
            .build();
    }

    private static HttpRequest.Builder request(int port, String path) {
        return HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
            .timeout(Duration.ofSeconds(5));
    }

    private static HttpResponse<String> send(HttpRequest.Builder request) {
        try {
            HttpResponse<String> response = CLIENT.send(request.build(), HttpResponse.BodyHandlers.ofString());
            assertThat(response, is(notNullValue()));

            return response;
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @FunctionalInterface
    private interface ServerAssertions {
        void run(List<Execution> executions) throws Exception;
    }
}
