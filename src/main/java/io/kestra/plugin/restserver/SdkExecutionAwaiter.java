package io.kestra.plugin.restserver;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.SDK;
import io.kestra.sdk.KestraClient;
import io.kestra.sdk.model.Execution;
import io.kestra.sdk.model.StateType;
import reactor.core.Disposable;

import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SPIKE — awaits terminal executions through Kestra's public API rather than its internal beans.
 * <p>
 * This is the shape Kestra advised in <a href="https://github.com/kestra-io/kestra/issues/17991">#17991</a>:
 * in 2.0 a Worker talks to Kestra over gRPC and has no repository beans of its own, so a plugin that needs
 * anything database-backed is expected to call the API. {@code followExecution} is the SSE endpoint the UI
 * follows, already wrapped as a {@code Flux} by the generated client.
 * <p>
 * Not wired into the trigger. It exists to measure what the migration costs; see {@code local-work} for the
 * findings. The two differences that matter against
 * {@link RestServerRealtimeTrigger.ExecutionAwaiter} are that this needs credentials and a reachable
 * webserver, and that it yields the API's {@link Execution} model rather than core's.
 */
final class SdkExecutionAwaiter implements AutoCloseable {

    /**
     * Kestra's OSS default tenant, used when the trigger context carries none.
     */
    private static final String DEFAULT_TENANT = "main";

    private static final String DEFAULT_URL = "http://localhost:8080";

    /**
     * Rendered when no explicit URL is configured — the convention {@code kestra-io/plugin-kestra} uses.
     */
    private static final String URL_TEMPLATE = "{{ kestra.url }}";

    /**
     * The states {@code followExecution} can leave an execution in once its stream ends. Only consulted as a
     * guard: the server completes the stream itself, so the last element is normally already terminal.
     */
    private static final Set<StateType> TERMINAL = EnumSet.of(
        StateType.SUCCESS,
        StateType.WARNING,
        StateType.FAILED,
        StateType.KILLED,
        StateType.CANCELLED,
        StateType.SKIPPED
    );

    private final KestraClient client;
    private final String tenantId;
    private final Map<String, Disposable> pending = new ConcurrentHashMap<>();

    private SdkExecutionAwaiter(KestraClient client, String tenantId) {
        this.client = client;
        this.tenantId = tenantId;
    }

    static SdkExecutionAwaiter open(RunContext runContext, Property<String> kestraUrl, String tenantId)
        throws IllegalVariableEvaluationException {

        KestraClient.KestraClientBuilder builder = KestraClient.builder().url(resolveUrl(runContext, kestraUrl));

        // Credentials come from kestra.tasks.sdk.authentication, the same source plugin-kestra falls back to.
        // Unlike a task, a realtime trigger has no user-facing step at which to prompt for them, so an
        // instance that has not configured them cannot serve a synchronous route at all.
        Optional<SDK.Auth> auth = runContext.sdk().defaultAuthentication();
        if (auth.isEmpty()) {
            throw new IllegalStateException(
                "Synchronous 'wait' mode needs Kestra API credentials. Set kestra.tasks.sdk.authentication.api-token, "
                    + "or .username and .password, on the instance running this trigger."
            );
        }

        SDK.Auth resolved = auth.get();
        if (resolved.apiToken().isPresent()) {
            builder.tokenAuth(resolved.apiToken().get());
        } else if (resolved.username().isPresent() && resolved.password().isPresent()) {
            builder.basicAuth(resolved.username().get(), resolved.password().get());
        } else {
            throw new IllegalStateException("Kestra API credentials are configured but incomplete.");
        }

        return new SdkExecutionAwaiter(builder.build(), tenantId == null ? DEFAULT_TENANT : tenantId);
    }

    private static String resolveUrl(RunContext runContext, Property<String> kestraUrl)
        throws IllegalVariableEvaluationException {

        String raw = runContext.render(kestraUrl).as(String.class).orElseGet(() -> {
            try {
                return runContext.render(URL_TEMPLATE);
            } catch (IllegalVariableEvaluationException e) {
                return DEFAULT_URL;
            }
        });

        return raw.trim().replaceAll("/+$", "");
    }

    /**
     * Unlike the streaming-service awaiter this needs no {@code Flow}: the server decides when following stops
     * and completes the stream, so the last element it emits is the terminal execution.
     */
    CompletableFuture<Execution> register(String executionId) {
        CompletableFuture<Execution> completion = new CompletableFuture<>();

        Disposable subscription = client.executions()
            .followExecution(tenantId, executionId)
            .filter(execution -> execution.getState() != null && TERMINAL.contains(execution.getState().getCurrent()))
            .next()
            .subscribe(completion::complete, completion::completeExceptionally);

        pending.put(executionId, subscription);
        completion.whenComplete((execution, throwable) -> pending.remove(executionId));

        return completion;
    }

    /**
     * Bridges the API's execution model to what {@code mapResponse} needs.
     * <p>
     * The generated model types {@code getOutputs()} as bare {@code Object} rather than a map, so the cast is
     * unavoidable; the API returns the outputs map as JSON, so it deserialises to exactly that.
     */
    @SuppressWarnings("unchecked")
    static RestServerRealtimeTrigger.ResponseSpec mapResponse(
        Execution execution,
        String responseOutputKey,
        String defaultContentType
    ) throws Exception {
        Object outputs = execution.getOutputs();
        boolean success = execution.getState() != null
            && (execution.getState().getCurrent() == StateType.SUCCESS
                || execution.getState().getCurrent() == StateType.WARNING);

        return RestServerRealtimeTrigger.mapResponse(
            outputs instanceof Map ? (Map<String, Object>) outputs : null,
            success,
            responseOutputKey,
            defaultContentType
        );
    }

    void cancel(String executionId) {
        Disposable subscription = pending.remove(executionId);
        if (subscription != null) {
            subscription.dispose();
        }
    }

    @Override
    public void close() {
        pending.values().forEach(Disposable::dispose);
        pending.clear();
    }
}
