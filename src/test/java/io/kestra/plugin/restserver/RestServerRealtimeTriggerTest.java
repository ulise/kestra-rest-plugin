package io.kestra.plugin.restserver;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.Trigger;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
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
import java.util.concurrent.CopyOnWriteArrayList;

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
