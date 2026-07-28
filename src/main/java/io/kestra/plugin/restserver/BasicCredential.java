package io.kestra.plugin.restserver;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

@Getter
@Builder
@Jacksonized
@Schema(title = "A username/password pair accepted by HTTP Basic authentication")
public class BasicCredential {

    @Schema(title = "Accepted username")
    @NotNull
    private Property<String> username;

    @Schema(
        title = "Accepted password",
        description = "Source it from a secret or KV; it is compared in constant time and never reaches the flow."
    )
    // Note: Kestra's `secret = true` is documentation metadata (it drives UI masking via
    // JsonSchemaGenerator), not runtime redaction, and it is not honoured on nested objects at all.
    // The real protection is that RestServerRealtimeTrigger strips `Authorization` from the trigger
    // variables whenever Basic auth is configured, so the password is never persisted.
    @PluginProperty(secret = true)
    @NotNull
    private Property<String> password;
}
