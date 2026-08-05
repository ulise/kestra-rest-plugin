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
import io.kestra.core.storages.StorageContext;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
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
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
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

    @Inject
    private StorageInterface storageInterface;

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
    void apiKeyIsCaseInsensitiveOnHeaderNameAndFailsClosed() {
        RestServerRealtimeTrigger.HandlerConfig config = apiKeyConfig("s3cret");

        assertThat(authenticate(config, Map.of("X-Api-Key", "s3cret")), is(RestServerRealtimeTrigger.AuthResult.AUTHORIZED));
        // Header names are case-insensitive: a normalised "x-api-key" must still match.
        assertThat(authenticate(config, Map.of("x-api-key", "s3cret")), is(RestServerRealtimeTrigger.AuthResult.AUTHORIZED));
        // Wrong value is rejected, and is distinguishable from no value at all.
        assertThat(authenticate(config, Map.of("X-Api-Key", "nope")), is(RestServerRealtimeTrigger.AuthResult.INVALID));
        // Missing header fails closed, never "no key required".
        assertThat(authenticate(config, Map.of()), is(RestServerRealtimeTrigger.AuthResult.MISSING));
    }

    @Test
    void apiKeyAcceptsAnyKeyInTheList() {
        RestServerRealtimeTrigger.HandlerConfig config = apiKeyConfig("key-aaa", "key-bbb", "key-ccc");
        // Any listed key is accepted.
        assertThat(authenticate(config, Map.of("X-Api-Key", "key-aaa")), is(RestServerRealtimeTrigger.AuthResult.AUTHORIZED));
        assertThat(authenticate(config, Map.of("X-Api-Key", "key-ccc")), is(RestServerRealtimeTrigger.AuthResult.AUTHORIZED));
        // A key not in the list is rejected.
        assertThat(authenticate(config, Map.of("X-Api-Key", "key-zzz")), is(RestServerRealtimeTrigger.AuthResult.INVALID));
    }

    @Test
    void openGateAuthorizesWhenNoSchemeIsConfigured() {
        RestServerRealtimeTrigger.HandlerConfig config = apiKeyConfig();

        assertThat(authenticate(config, Map.of()), is(RestServerRealtimeTrigger.AuthResult.AUTHORIZED));
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
    // #10 HTTP Basic authentication
    // -------------------------------------------------------------------------------------------------------------

    @Test
    void parseBasicAcceptsAnyCaseSchemeAndRejectsWhatCannotBeCompared() {
        assertThat(RestServerRealtimeTrigger.parseBasic("Basic " + base64("alice:s3cret")),
            is(new RestServerRealtimeTrigger.BasicCredentials("alice", "s3cret")));
        // RFC 9110 makes the scheme token case-insensitive; Javalin's own helper matches "Basic " exactly.
        assertThat(RestServerRealtimeTrigger.parseBasic("basic " + base64("alice:s3cret")),
            is(new RestServerRealtimeTrigger.BasicCredentials("alice", "s3cret")));
        assertThat(RestServerRealtimeTrigger.parseBasic("BASIC " + base64("alice:s3cret")),
            is(new RestServerRealtimeTrigger.BasicCredentials("alice", "s3cret")));
        // Only the first colon separates, so a password may contain one.
        assertThat(RestServerRealtimeTrigger.parseBasic("Basic " + base64("alice:pa:ss")),
            is(new RestServerRealtimeTrigger.BasicCredentials("alice", "pa:ss")));
        // An empty password is well-formed and must still be compared rather than treated as absent.
        assertThat(RestServerRealtimeTrigger.parseBasic("Basic " + base64("alice:")),
            is(new RestServerRealtimeTrigger.BasicCredentials("alice", "")));

        // Nothing that can be compared: absent, another scheme, undecodable, or no colon at all.
        assertThat(RestServerRealtimeTrigger.parseBasic(null), is(nullValue()));
        assertThat(RestServerRealtimeTrigger.parseBasic("Bearer " + base64("alice:s3cret")), is(nullValue()));
        assertThat(RestServerRealtimeTrigger.parseBasic("Basic"), is(nullValue()));
        assertThat(RestServerRealtimeTrigger.parseBasic("Basic !!!not-base64!!!"), is(nullValue()));
        assertThat(RestServerRealtimeTrigger.parseBasic("Basic " + base64("no-colon-here")), is(nullValue()));
    }

    @Test
    void basicAuthGuardsRequestsBeforeAnyExecution() throws Exception {
        int port = freePort();
        RestServerRealtimeTrigger trigger = basicAuthTrigger(port, null, credential("alice", "s3cret"));

        withRunningServer(trigger, port, executions -> {
            // No credentials at all.
            HttpResponse<String> missing = send(request(port, "/api/orders").GET());
            assertThat(missing.statusCode(), is(401));
            // RFC 9110: a 401 has to say how to authenticate.
            assertThat(missing.headers().firstValue("WWW-Authenticate").orElse(""), is("Basic"));

            // Another scheme, and an undecodable payload, are both "no usable credentials".
            assertThat(send(request(port, "/api/orders").header("Authorization", "Bearer abc").GET()).statusCode(), is(401));
            assertThat(send(request(port, "/api/orders").header("Authorization", "Basic !!!").GET()).statusCode(), is(401));
            // Wrong password, and wrong user, default to 401 as well.
            assertThat(send(request(port, "/api/orders").header("Authorization", basic("alice", "nope")).GET()).statusCode(), is(401));
            assertThat(send(request(port, "/api/orders").header("Authorization", basic("mallory", "s3cret")).GET()).statusCode(), is(401));

            // None of the rejected requests started an execution — the whole point of the edge check.
            assertThat(executions, is(empty()));

            // Correct credentials, with a lower-cased header name and a lower-cased scheme token.
            assertThat(send(request(port, "/api/orders").header("authorization", "basic " + base64("alice:s3cret")).GET()).statusCode(), is(202));
            await().atMost(Duration.ofSeconds(5)).until(() -> executions.size() == 1);
        });
    }

    @Test
    void invalidCredentialsStatusSeparatesWrongFromAbsent() throws Exception {
        int port = freePort();
        RestServerRealtimeTrigger trigger = basicAuthTrigger(port, 403, credential("alice", "s3cret"));

        withRunningServer(trigger, port, executions -> {
            // Absent or unparseable stays 401: nothing could be compared.
            assertThat(send(request(port, "/api/orders").GET()).statusCode(), is(401));
            assertThat(send(request(port, "/api/orders").header("Authorization", "Bearer abc").GET()).statusCode(), is(401));
            assertThat(send(request(port, "/api/orders").header("Authorization", "Basic !!!").GET()).statusCode(), is(401));

            // Well-formed but wrong is the case that gets the configured status.
            HttpResponse<String> wrong = send(request(port, "/api/orders").header("Authorization", basic("alice", "nope")).GET());
            assertThat(wrong.statusCode(), is(403));
            assertThat(wrong.body(), containsString("\"status\":\"FORBIDDEN\""));
            // A 403 is not a challenge, so it carries no WWW-Authenticate.
            assertThat(wrong.headers().firstValue("WWW-Authenticate").isPresent(), is(false));

            assertThat(executions, is(empty()));
        });
    }

    @Test
    void authFailureBodyIsReturnedVerbatimForBothStatuses() throws Exception {
        int port = freePort();
        RestServerRealtimeTrigger trigger = RestServerRealtimeTrigger.builder()
            .id("rest_server")
            .type(RestServerRealtimeTrigger.class.getName())
            .port(Property.ofValue(port))
            .basePath(Property.ofValue("/api"))
            .basicAuth(List.of(credential("alice", "s3cret")))
            .invalidCredentialsStatus(Property.ofValue(403))
            .authFailureBody(Property.ofValue("{}"))
            .routes(List.of(route("GET", "/orders", null, null)))
            .build();

        withRunningServer(trigger, port, executions -> {
            HttpResponse<String> missing = send(request(port, "/api/orders").GET());
            assertThat(missing.statusCode(), is(401));
            assertThat(missing.body(), is("{}"));

            HttpResponse<String> wrong = send(request(port, "/api/orders").header("Authorization", basic("alice", "nope")).GET());
            assertThat(wrong.statusCode(), is(403));
            assertThat(wrong.body(), is("{}"));
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    void basicAuthStripsCredentialsAndExposesTheMatchedUsername() throws Exception {
        int port = freePort();
        RestServerRealtimeTrigger trigger = basicAuthTrigger(port, null,
            credential("alice", "s3cret"), credential("bob", "hunter2"));

        withRunningServer(trigger, port, executions -> {
            // Two different callers are both accepted from one list.
            assertThat(send(request(port, "/api/orders").header("Authorization", basic("alice", "s3cret")).GET()).statusCode(), is(202));
            assertThat(send(request(port, "/api/orders").header("Authorization", basic("bob", "hunter2")).GET()).statusCode(), is(202));
            // A pair that is not listed is rejected, even though each half exists on its own.
            assertThat(send(request(port, "/api/orders").header("Authorization", basic("alice", "hunter2")).GET()).statusCode(), is(401));

            await().atMost(Duration.ofSeconds(5)).until(() -> executions.size() == 2);

            RestServerRealtimeTrigger.Output output = output(executions.getFirst());
            // The flow learns who called...
            assertThat(output.getBasicAuthUser(), is("alice"));
            // ...but the password never reaches the execution, where it would be persisted in the clear.
            assertThat(output.getHeaders().keySet().stream().anyMatch(k -> k.equalsIgnoreCase("Authorization")), is(false));

            Map<String, Object> vars = (Map<String, Object>) executions.getFirst().getTrigger().getVariables();
            Map<String, String> headers = (Map<String, String>) vars.get("headers");
            assertThat(headers.keySet().stream().anyMatch(k -> k.equalsIgnoreCase("Authorization")), is(false));
            assertThat(vars.get("basicAuthUser"), is("alice"));
        });
    }

    @Test
    void basicAuthAndApiKeyCombineWithOr() throws Exception {
        int port = freePort();
        RestServerRealtimeTrigger trigger = RestServerRealtimeTrigger.builder()
            .id("rest_server")
            .type(RestServerRealtimeTrigger.class.getName())
            .port(Property.ofValue(port))
            .basePath(Property.ofValue("/api"))
            .apiKey(Property.ofValue("s3cret-key"))
            .basicAuth(List.of(credential("alice", "s3cret")))
            .routes(List.of(route("GET", "/orders", null, null)))
            .build();

        withRunningServer(trigger, port, executions -> {
            // Either scheme on its own is enough.
            assertThat(send(request(port, "/api/orders").header("X-Api-Key", "s3cret-key").GET()).statusCode(), is(202));
            assertThat(send(request(port, "/api/orders").header("Authorization", basic("alice", "s3cret")).GET()).statusCode(), is(202));
            // Neither satisfied.
            assertThat(send(request(port, "/api/orders").header("X-Api-Key", "nope").GET()).statusCode(), is(401));
            assertThat(send(request(port, "/api/orders").GET()).statusCode(), is(401));

            await().atMost(Duration.ofSeconds(5)).until(() -> executions.size() == 2);
            // With Basic configured, the API-key header is still forwarded — only Authorization is stripped.
            assertThat(output(executions.getFirst()).getHeaders().entrySet().stream()
                .anyMatch(e -> e.getKey().equalsIgnoreCase("X-Api-Key")), is(true));
        });
    }

    @Test
    void headersReachTheFlowUnchangedWhenBasicAuthIsNotConfigured() throws Exception {
        int port = freePort();
        RestServerRealtimeTrigger trigger = trigger(port, "/api", route("GET", "/orders", null, null));

        withRunningServer(trigger, port, executions -> {
            // Without basicAuth the trigger must not start filtering headers it never used to touch.
            assertThat(send(request(port, "/api/orders").header("Authorization", "Bearer opaque-token").GET()).statusCode(), is(202));

            await().atMost(Duration.ofSeconds(5)).until(() -> executions.size() == 1);
            RestServerRealtimeTrigger.Output output = output(executions.getFirst());
            assertThat(output.getHeaders().entrySet().stream()
                .filter(e -> e.getKey().equalsIgnoreCase("Authorization"))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null), is("Bearer opaque-token"));
            assertThat(output.getBasicAuthUser(), is(nullValue()));
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

    // -------------------------------------------------------------------------------------------------------------
    // #8 Multipart and binary bodies
    // -------------------------------------------------------------------------------------------------------------

    @Test
    @SuppressWarnings("unchecked")
    void multipartStoresFilePartsIntactAndExposesFormFields() throws Exception {
        int port = freePort();
        RestServerRealtimeTrigger trigger = trigger(port, "/api", route("POST", "/upload", null, null));

        // Bytes that are not valid UTF-8 — they would be mangled by string decoding.
        byte[] photo = {(byte) 0xFF, (byte) 0xD8, (byte) 0xFF, (byte) 0xE0, 0x00, 0x10, (byte) 0x89, (byte) 0xC3, 0x7F, (byte) 0x80};
        String boundary = "----testBoundaryXYZ";
        byte[] body = multipartBody(boundary,
            new PartSpec("photo", "result.jpg", "image/jpeg", photo),
            new PartSpec("note", null, null, "hello world".getBytes(StandardCharsets.UTF_8)));

        withRunningServer(trigger, port, executions -> {
            HttpResponse<String> response = send(request(port, "/api/upload")
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body)));
            assertThat(response.statusCode(), is(202));

            await().atMost(Duration.ofSeconds(5)).until(() -> executions.size() == 1);
            Execution execution = executions.getFirst();
            Map<String, Object> vars = (Map<String, Object>) execution.getTrigger().getVariables();

            List<Map<String, Object>> parts = (List<Map<String, Object>>) vars.get("parts");
            assertThat(parts, hasSize(1));
            Map<String, Object> part = parts.getFirst();
            assertThat(part.get("name"), is("photo"));
            assertThat(part.get("filename"), is("result.jpg"));
            assertThat(part.get("contentType"), is("image/jpeg"));
            assertThat(((Number) part.get("size")).longValue(), is((long) photo.length));
            // No bytes in the execution: the part is reached by URI, and reads back byte-identical.
            assertThat(part.get("content"), is(nullValue()));
            URI uri = URI.create((String) part.get("uri"));
            assertThat(uri.getScheme(), is("kestra"));
            assertThat(uri.getPath(), allOf(
                containsString("/executions/" + execution.getId() + "/webhook/0/"),
                containsString("result.jpg")
            ));
            assertThat(read(execution, uri), is(photo));

            Map<String, List<String>> formFields = (Map<String, List<String>>) vars.get("formFields");
            assertThat(formFields.get("note").getFirst(), is("hello world"));
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    void multipartNumbersPartsSharingAFilename() throws Exception {
        int port = freePort();
        RestServerRealtimeTrigger trigger = trigger(port, "/api", route("POST", "/upload", null, null));

        String boundary = "----testBoundarySameName";
        byte[] body = multipartBody(boundary,
            new PartSpec("photo", "result.jpg", "image/jpeg", "first".getBytes(StandardCharsets.UTF_8)),
            new PartSpec("photo", "result.jpg", "image/jpeg", "second".getBytes(StandardCharsets.UTF_8)));

        withRunningServer(trigger, port, executions -> {
            send(request(port, "/api/upload")
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body)));

            await().atMost(Duration.ofSeconds(5)).until(() -> executions.size() == 1);
            Execution execution = executions.getFirst();
            Map<String, Object> vars = (Map<String, Object>) execution.getTrigger().getVariables();
            List<Map<String, Object>> parts = (List<Map<String, Object>>) vars.get("parts");

            // Same field name and same filename, so only the numbering keeps them apart.
            assertThat(parts, hasSize(2));
            assertThat(new String(read(execution, URI.create((String) parts.get(0).get("uri"))), StandardCharsets.UTF_8), is("first"));
            assertThat(new String(read(execution, URI.create((String) parts.get(1).get("uri"))), StandardCharsets.UTF_8), is("second"));
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    void multipartKeepsOnlyTheFileNameOfATraversingPart() throws Exception {
        int port = freePort();
        RestServerRealtimeTrigger trigger = trigger(port, "/api", route("POST", "/upload", null, null));

        String boundary = "----testBoundaryTraversal";
        byte[] body = multipartBody(boundary,
            new PartSpec("photo", "../../evil.jpg", "image/jpeg", "payload".getBytes(StandardCharsets.UTF_8)));

        withRunningServer(trigger, port, executions -> {
            send(request(port, "/api/upload")
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body)));

            await().atMost(Duration.ofSeconds(5)).until(() -> executions.size() == 1);
            Execution execution = executions.getFirst();
            Map<String, Object> vars = (Map<String, Object>) execution.getTrigger().getVariables();
            String uri = (String) ((List<Map<String, Object>>) vars.get("parts")).getFirst().get("uri");

            // The part cannot be written outside the execution's own directory.
            assertThat(uri, containsString("/executions/" + execution.getId() + "/webhook/0/evil.jpg"));
            assertThat(uri, not(containsString("..")));
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    void multipartStoresAPartWhoseFilenameNeedsUriEncoding() throws Exception {
        int port = freePort();
        RestServerRealtimeTrigger trigger = trigger(port, "/api", route("POST", "/upload", null, null));

        // A space is illegal in a URI path, so building the storage URI used to throw and fail the whole upload.
        byte[] content = {(byte) 0xFF, (byte) 0xD8, 0x00, (byte) 0x89, 0x7F};
        String boundary = "----testBoundarySpacedName";
        byte[] body = multipartBody(boundary,
            new PartSpec("report", "My Report.pdf", "application/pdf", content));

        withRunningServer(trigger, port, executions -> {
            HttpResponse<String> response = send(request(port, "/api/upload")
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body)));
            assertThat(response.statusCode(), is(202));

            await().atMost(Duration.ofSeconds(5)).until(() -> executions.size() == 1);
            Execution execution = executions.getFirst();
            Map<String, Object> vars = (Map<String, Object>) execution.getTrigger().getVariables();
            Map<String, Object> part = ((List<Map<String, Object>>) vars.get("parts")).getFirst();

            // The flow still sees the name the caller gave, spaces and all.
            assertThat(part.get("filename"), is("My Report.pdf"));

            URI uri = URI.create((String) part.get("uri"));
            assertThat(uri.getScheme(), is("kestra"));
            // Encoded in the URI, decoded in the path the storage resolves it by.
            assertThat(uri.getRawPath(), containsString("My%20Report.pdf"));
            assertThat(uri.getPath(), containsString("/webhook/0/My Report.pdf"));
            assertThat(read(execution, uri), is(content));
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    void multipartStoresPartsWhateverTheCallerNamesThem() throws Exception {
        int port = freePort();
        RestServerRealtimeTrigger trigger = trigger(port, "/api", route("POST", "/upload", null, null));

        // Every one of these is either reserved in a URI or ambiguous with a scheme; a filename is caller-controlled,
        // so none of them may fail the upload.
        List<String> names = List.of("Q3 summary (final).csv", "a#b.txt", "a?b.txt", "50%.txt", "a:b.txt", ":lead.txt");

        String boundary = "----testBoundaryAwkwardNames";
        PartSpec[] specs = names.stream()
            .map(name -> new PartSpec("file", name, "text/plain", name.getBytes(StandardCharsets.UTF_8)))
            .toArray(PartSpec[]::new);

        withRunningServer(trigger, port, executions -> {
            assertThat(send(request(port, "/api/upload")
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofByteArray(multipartBody(boundary, specs)))).statusCode(), is(202));

            await().atMost(Duration.ofSeconds(5)).until(() -> executions.size() == 1);
            Execution execution = executions.getFirst();
            Map<String, Object> vars = (Map<String, Object>) execution.getTrigger().getVariables();
            List<Map<String, Object>> parts = (List<Map<String, Object>>) vars.get("parts");

            assertThat(parts, hasSize(names.size()));
            for (int i = 0; i < names.size(); i++) {
                String name = names.get(i);
                Map<String, Object> part = parts.get(i);

                // The flow is told the name the caller sent, whatever the storage then makes of it.
                assertThat(part.get("filename"), is(name));

                URI uri = URI.create((String) part.get("uri"));
                // Kestra strips colons from a storage path — WindowsUtils.windowsToUnixPath normalises drive
                // letters and does not distinguish them from a colon anywhere else — so that is the one character
                // the stored name cannot keep. Everything else survives.
                assertThat(uri.getPath(), containsString("/webhook/" + i + "/" + name.replace(":", "")));
                // Whatever it was stored as, the part reads back whole.
                assertThat(new String(read(execution, uri), StandardCharsets.UTF_8), is(name));
            }
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    void fetchTypeStoreStreamsBinaryBodyToStorage() throws Exception {
        int port = freePort();
        RestServerRealtimeTrigger trigger = trigger(port, "/api",
            storeRoute("POST", "/blob"));

        byte[] blob = {0x00, (byte) 0xFF, 0x10, (byte) 0x80, 0x7F, (byte) 0xC3};

        withRunningServer(trigger, port, executions -> {
            HttpResponse<String> response = send(request(port, "/api/blob")
                .header("Content-Type", "application/octet-stream")
                .POST(HttpRequest.BodyPublishers.ofByteArray(blob)));
            assertThat(response.statusCode(), is(202));

            await().atMost(Duration.ofSeconds(5)).until(() -> executions.size() == 1);
            Execution execution = executions.getFirst();
            Map<String, Object> vars = (Map<String, Object>) execution.getTrigger().getVariables();

            // The body never reaches the execution — only the URI it was stored under does.
            assertThat(vars.get("body"), is(nullValue()));
            assertThat(vars.get("bodyBase64"), is(nullValue()));
            URI uri = URI.create((String) vars.get("uri"));
            assertThat(uri.getPath(), containsString("/executions/" + execution.getId() + "/webhook/body"));
            assertThat(read(execution, uri), is(blob));
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    void fetchTypeStoreLeavesNothingBehindForAnEmptyBody() throws Exception {
        int port = freePort();
        RestServerRealtimeTrigger trigger = trigger(port, "/api", storeRoute("POST", "/blob"));

        withRunningServer(trigger, port, executions -> {
            HttpResponse<String> response = send(request(port, "/api/blob")
                .POST(HttpRequest.BodyPublishers.noBody()));
            assertThat(response.statusCode(), is(202));

            await().atMost(Duration.ofSeconds(5)).until(() -> executions.size() == 1);
            Execution execution = executions.getFirst();
            Map<String, Object> vars = (Map<String, Object>) execution.getTrigger().getVariables();

            // No URI, and no empty file left in the storage either.
            assertThat(vars.get("uri"), is(nullValue()));
            assertThat(storedFiles(execution), is(empty()));
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    void fetchTypeNoneDropsTheBody() throws Exception {
        int port = freePort();
        RestServerRealtimeTrigger trigger = trigger(port, "/api",
            RouteDefinition.builder()
                .method(Property.ofValue("POST"))
                .path(Property.ofValue("/ping"))
                .fetchType(Property.ofValue(FetchType.NONE))
                .build());

        withRunningServer(trigger, port, executions -> {
            HttpResponse<String> response = send(request(port, "/api/ping")
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"item\":\"widget\"}")));
            assertThat(response.statusCode(), is(202));

            await().atMost(Duration.ofSeconds(5)).until(() -> executions.size() == 1);
            Execution execution = executions.getFirst();
            Map<String, Object> vars = (Map<String, Object>) execution.getTrigger().getVariables();

            assertThat(vars.get("body"), is(nullValue()));
            assertThat(vars.get("uri"), is(nullValue()));
            assertThat(storedFiles(execution), is(empty()));
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    void routeFetchTypeOverridesTheTriggerDefault() throws Exception {
        int port = freePort();
        RestServerRealtimeTrigger trigger = RestServerRealtimeTrigger.builder()
            .id("rest_server")
            .type(RestServerRealtimeTrigger.class.getName())
            .port(Property.ofValue(port))
            .basePath(Property.ofValue("/api"))
            .fetchType(Property.ofValue(FetchType.STORE))
            .routes(List.of(
                route("POST", "/stored", null, null),
                RouteDefinition.builder()
                    .method(Property.ofValue("POST"))
                    .path(Property.ofValue("/fetched"))
                    .fetchType(Property.ofValue(FetchType.FETCH))
                    .build()
            ))
            .build();

        withRunningServer(trigger, port, executions -> {
            send(request(port, "/api/stored").POST(HttpRequest.BodyPublishers.ofString("stored payload")));
            await().atMost(Duration.ofSeconds(5)).until(() -> executions.size() == 1);

            send(request(port, "/api/fetched").POST(HttpRequest.BodyPublishers.ofString("fetched payload")));
            await().atMost(Duration.ofSeconds(5)).until(() -> executions.size() == 2);

            // The route without its own fetchType inherits STORE from the trigger; the other one opts back out.
            Map<String, Object> stored = (Map<String, Object>) executions.get(0).getTrigger().getVariables();
            assertThat(stored.get("body"), is(nullValue()));
            assertThat(new String(read(executions.get(0), URI.create((String) stored.get("uri"))), StandardCharsets.UTF_8),
                is("stored payload"));

            Map<String, Object> fetched = (Map<String, Object>) executions.get(1).getTrigger().getVariables();
            assertThat(fetched.get("body"), is("fetched payload"));
            assertThat(fetched.get("uri"), is(nullValue()));
        });
    }

    @Test
    void rejectedRequestStoresNothing() throws Exception {
        int port = freePort();
        RestServerRealtimeTrigger trigger = RestServerRealtimeTrigger.builder()
            .id("rest_server")
            .type(RestServerRealtimeTrigger.class.getName())
            .port(Property.ofValue(port))
            .basePath(Property.ofValue("/api"))
            .apiKey(Property.ofValue("s3cret"))
            .routes(List.of(storeRoute("POST", "/blob")))
            .build();

        withRunningServer(trigger, port, executions -> {
            // Rejected by the key, and by the content type — neither reaches the body, so neither stores.
            assertThat(send(request(port, "/api/blob")
                .POST(HttpRequest.BodyPublishers.ofString("payload"))).statusCode(), is(401));

            await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2)).until(executions::isEmpty);
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    void base64BodyRouteRoundTripsBinaryBody() throws Exception {
        int port = freePort();
        RestServerRealtimeTrigger trigger = RestServerRealtimeTrigger.builder()
            .id("rest_server")
            .type(RestServerRealtimeTrigger.class.getName())
            .port(Property.ofValue(port))
            .basePath(Property.ofValue("/api"))
            .routes(List.of(RouteDefinition.builder()
                .method(Property.ofValue("POST"))
                .path(Property.ofValue("/blob"))
                .base64Body(Property.ofValue(true))
                .build()))
            .build();

        byte[] blob = {0x00, (byte) 0xFF, 0x10, (byte) 0x80, 0x7F, (byte) 0xC3};

        withRunningServer(trigger, port, executions -> {
            HttpResponse<String> response = send(request(port, "/api/blob")
                .header("Content-Type", "application/octet-stream")
                .POST(HttpRequest.BodyPublishers.ofByteArray(blob)));
            assertThat(response.statusCode(), is(202));

            await().atMost(Duration.ofSeconds(5)).until(() -> executions.size() == 1);
            Map<String, Object> vars = (Map<String, Object>) executions.getFirst().getTrigger().getVariables();
            assertThat(Base64.getDecoder().decode((String) vars.get("bodyBase64")), is(blob));
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    void nonMultipartWithoutFlagExposesNoBase64OrParts() throws Exception {
        int port = freePort();
        RestServerRealtimeTrigger trigger = trigger(port, "/api", route("POST", "/orders", "application/json", null));

        withRunningServer(trigger, port, executions -> {
            send(request(port, "/api/orders").header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"item\":\"widget\"}")));

            await().atMost(Duration.ofSeconds(5)).until(() -> executions.size() == 1);
            Map<String, Object> vars = (Map<String, Object>) executions.getFirst().getTrigger().getVariables();
            // Text/JSON is unchanged: the string body is present, the binary extras are not.
            assertThat(vars.get("body"), is("{\"item\":\"widget\"}"));
            assertThat(vars.get("bodyBase64"), is(nullValue()));
            assertThat(vars.get("parts"), is(nullValue()));
        });
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Reads back what the trigger stored for an execution, so a test can assert the bytes round-tripped.
     */
    private byte[] read(Execution execution, URI uri) throws IOException {
        try (InputStream content = storageInterface.get(execution.getTenantId(), execution.getNamespace(), uri)) {
            return content.readAllBytes();
        }
    }

    /**
     * Everything stored under an execution, used to assert that a request left nothing behind.
     */
    private List<URI> storedFiles(Execution execution) throws IOException {
        URI prefix = StorageContext
            .forExecution(execution.getTenantId(), execution.getNamespace(), execution.getFlowId(), execution.getId())
            .getContextStorageURI();

        try {
            return storageInterface.allByPrefix(execution.getTenantId(), execution.getNamespace(), prefix, false);
        } catch (FileNotFoundException e) {
            // Nothing was ever written for this execution, which is exactly what the caller is asserting.
            return List.of();
        }
    }

    /**
     * Builds a {@code multipart/form-data} body for the given parts, matching the supplied boundary.
     */
    private static byte[] multipartBody(String boundary, PartSpec... parts) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (PartSpec part : parts) {
            out.write(("--" + boundary + "\r\n").getBytes(StandardCharsets.UTF_8));
            StringBuilder header = new StringBuilder("Content-Disposition: form-data; name=\"").append(part.name).append('"');
            if (part.filename != null) {
                header.append("; filename=\"").append(part.filename).append('"');
            }
            header.append("\r\n");
            out.write(header.toString().getBytes(StandardCharsets.UTF_8));
            if (part.contentType != null) {
                out.write(("Content-Type: " + part.contentType + "\r\n").getBytes(StandardCharsets.UTF_8));
            }
            out.write("\r\n".getBytes(StandardCharsets.UTF_8));
            out.write(part.content);
            out.write("\r\n".getBytes(StandardCharsets.UTF_8));
        }
        out.write(("--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8));

        return out.toByteArray();
    }

    private record PartSpec(String name, String filename, String contentType, byte[] content) {
    }

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
            .basicAuthUser((String) variables.get("basicAuthUser"))
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

    private static RestServerRealtimeTrigger basicAuthTrigger(int port, Integer invalidStatus, BasicCredential... credentials) {
        RestServerRealtimeTrigger.RestServerRealtimeTriggerBuilder<?, ?> builder = RestServerRealtimeTrigger.builder()
            .id("rest_server")
            .type(RestServerRealtimeTrigger.class.getName())
            .port(Property.ofValue(port))
            .basePath(Property.ofValue("/api"))
            .basicAuth(List.of(credentials))
            .routes(List.of(route("GET", "/orders", null, null)));

        if (invalidStatus != null) {
            builder.invalidCredentialsStatus(Property.ofValue(invalidStatus));
        }

        return builder.build();
    }

    private static BasicCredential credential(String username, String password) {
        return BasicCredential.builder()
            .username(Property.ofValue(username))
            .password(Property.ofValue(password))
            .build();
    }

    /** Mirrors what {@code handle()} does: parse the header only when Basic is configured, then run the gate. */
    private static RestServerRealtimeTrigger.AuthResult authenticate(
        RestServerRealtimeTrigger.HandlerConfig config,
        Map<String, String> headers
    ) {
        RestServerRealtimeTrigger.BasicCredentials basic = config.basicDigests().isEmpty()
            ? null
            : RestServerRealtimeTrigger.parseBasic(RestServerRealtimeTrigger.header(headers, "Authorization"));

        return RestServerRealtimeTrigger.authenticate(headers, basic, config);
    }

    private static RestServerRealtimeTrigger.HandlerConfig apiKeyConfig(String... keys) {
        return new RestServerRealtimeTrigger.HandlerConfig(
            "X-Api-Key", List.of(keys), List.of(), 401, null, "response", Duration.ofSeconds(30));
    }

    private static String basic(String username, String password) {
        return "Basic " + base64(username + ":" + password);
    }

    private static String base64(String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static RouteDefinition storeRoute(String method, String path) {
        return RouteDefinition.builder()
            .method(Property.ofValue(method))
            .path(Property.ofValue(path))
            .fetchType(Property.ofValue(FetchType.STORE))
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
