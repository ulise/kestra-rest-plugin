package io.kestra.plugin.restserver;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

@Getter
@Builder
@Jacksonized
@Schema(title = "A single REST route exposed by the embedded server")
public class RouteDefinition {

    @Schema(
        title = "HTTP method",
        description = "One of `GET`, `POST`, `PUT`, `PATCH`, `DELETE`, `HEAD`, `OPTIONS`. Case-insensitive."
    )
    @NotNull
    private Property<String> method;

    @Schema(
        title = "Path relative to `basePath`",
        description = """
            Supports Javalin path parameters (`/orders/{id}`) and wildcards (`/files/<path>`).
            Extracted parameters are exposed as `{{ trigger.pathParams }}`."""
    )
    @NotNull
    private Property<String> path;

    @Schema(
        title = "Expected request `Content-Type`",
        description = """
            When set, requests whose `Content-Type` does not start with this value are rejected with
            `415 Unsupported Media Type` and no execution is created. When null, any content type is accepted."""
    )
    private Property<String> consumes;

    @Schema(
        title = "`Content-Type` of the acknowledgement response",
        description = "Defaults to `application/json`, which matches the acknowledgement body the trigger returns."
    )
    private Property<String> produces;

    @Schema(
        title = "Wait for the triggered execution and return its result",
        description = """
            Overrides the trigger-level `wait`. When `true`, the request blocks until the triggered execution
            reaches a terminal state and the response is built from the execution's outputs (see the trigger's
            `responseOutput`). When `false`, the request is answered immediately with `202 Accepted`. When null,
            the trigger-level `wait` applies."""
    )
    private Property<Boolean> wait;

    @Schema(
        title = "What to do with the request body",
        description = """
            Overrides the trigger-level `fetchType`. One of:

            - `FETCH` (the default): the body reaches the flow as `{{ trigger.body }}`, decoded as a string.
            - `STORE`: the body is streamed into Kestra's internal storage as it is received, and the flow
              reaches it as `{{ trigger.uri }}`. Nothing of it travels through the execution, so use it for
              uploads of any size, and for binary bodies that a string would corrupt.
            - `NONE`: the body is read off the connection and dropped.

            File parts of a `multipart/form-data` request are stored whatever this says — see
            `{{ trigger.parts }}`. When null, the trigger-level `fetchType` applies."""
    )
    private Property<FetchType> fetchType;

    @Schema(
        title = "Expose the raw body base64-encoded",
        description = """
            When `true`, the request body is also exposed as `{{ trigger.bodyBase64 }}` (base64 of the raw
            bytes), so binary bodies such as `application/octet-stream` survive intact. Only applies when the
            body is fetched: it is ignored for `fetchType: STORE` and `NONE`, and for `multipart/form-data`
            requests, whose file parts are always exposed via `{{ trigger.parts }}`.

            Deprecated in favour of `fetchType: STORE`, which keeps the payload out of the execution entirely
            instead of inflating it by a third. It still works, and will be removed in a future release."""
    )
    @Deprecated(since = "1.4.0", forRemoval = true)
    private Property<Boolean> base64Body;
}
