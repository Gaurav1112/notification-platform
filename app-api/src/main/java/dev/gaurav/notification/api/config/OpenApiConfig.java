package dev.gaurav.notification.api.config;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * The OpenAPI 3.1 document served at {@code /v3/api-docs} and committed to the repository.
 *
 * <p><strong>Committing the generated document is the point.</strong> A contract that only exists
 * at runtime cannot be reviewed: a rename that breaks every client shows up as a green build and a
 * broken integration two weeks later. Checked in, the same rename is a diff with the old and new
 * field names side by side, in the pull request that caused it.
 *
 * <p>The security scheme is declared as a global requirement with per-endpoint exceptions rather
 * than the other way round. Defaulting to "secured" means a new endpoint is documented as requiring
 * a token unless somebody deliberately says otherwise — the safe direction for a mistake.
 */
@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI notificationPlatformOpenApi() {
        var bearer = new SecurityScheme()
                .type(SecurityScheme.Type.HTTP)
                .scheme("bearer")
                .bearerFormat("JWT")
                .description("""
                        OAuth2 client-credentials, RS256. Scopes: notifications:send, \
                        notifications:read, providers:read, admin:*.
                        Inbound webhooks under /v1/webhooks are authenticated by HMAC over the raw \
                        body instead, and take no bearer token.""");

        return new OpenAPI()
                .info(new Info()
                        .title("Notification Platform API")
                        .version("1.0.0")
                        .description("""
                                Multi-channel notification delivery: SMS, e-mail and push.

                                Three conventions worth reading before integrating:

                                * `POST /v1/notifications` answers **202**, not 200. Nothing has been \
                                delivered yet; the request is durably queued.
                                * `Idempotency-Key` is **required** on requests that create work. The same \
                                key with a different body is a `409`, not a silent replay.
                                * Scheduled sends **must** carry a UTC offset. `2026-09-01T09:00:00` is a \
                                `400`, because nine o'clock in an unstated zone is a guess.

                                Errors are RFC 9457 `application/problem+json`; branch on the `type` URI, \
                                never on the English `title`.""")
                        .contact(new Contact().name("Notification Platform").email("platform@notification-platform.dev"))
                        .license(new License().name("Proprietary")))
                .servers(List.of(
                        new Server().url("http://localhost:8080").description("Local"),
                        new Server().url("https://api.notification-platform.dev").description("Production")))
                .components(new Components().addSecuritySchemes("bearerAuth", bearer))
                .addSecurityItem(new SecurityRequirement().addList("bearerAuth"));
    }
}
