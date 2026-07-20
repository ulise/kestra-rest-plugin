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
@Schema(title = "A single REST route exposed by the embedded server.")
public class RouteDefinition {

    @Schema(
        title = "HTTP method.",
        description = "One of `GET`, `POST`, `PUT`, `PATCH`, `DELETE`, `HEAD`, `OPTIONS`. Case-insensitive."
    )
    @NotNull
    private Property<String> method;

    @Schema(
        title = "Path relative to `basePath`.",
        description = """
            Supports Javalin path parameters (`/orders/{id}`) and wildcards (`/files/<path>`).
            Extracted parameters are exposed as `{{ trigger.pathParams }}`."""
    )
    @NotNull
    private Property<String> path;

    @Schema(
        title = "Expected request `Content-Type`.",
        description = """
            When set, requests whose `Content-Type` does not start with this value are rejected with
            `415 Unsupported Media Type` and no execution is created. When null, any content type is accepted."""
    )
    private Property<String> consumes;

    @Schema(
        title = "`Content-Type` of the acknowledgement response.",
        description = "Defaults to `application/json`, which matches the acknowledgement body the trigger returns."
    )
    private Property<String> produces;
}
