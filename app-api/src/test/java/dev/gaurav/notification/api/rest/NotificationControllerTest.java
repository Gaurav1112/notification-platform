package dev.gaurav.notification.api.rest;

import dev.gaurav.notification.api.config.ApiSecurityProperties;
import dev.gaurav.notification.api.dto.AcceptResponse;
import dev.gaurav.notification.api.error.AlreadyDispatchedException;
import dev.gaurav.notification.api.error.ApiExceptionHandler;
import dev.gaurav.notification.api.error.IdempotencyKeyReusedException;
import dev.gaurav.notification.api.error.NotificationNotFoundException;
import dev.gaurav.notification.api.filter.ApiCallerArgumentResolver;
import dev.gaurav.notification.api.filter.IdempotencyKeyInterceptor;
import dev.gaurav.notification.api.filter.PayloadSizeInterceptor;
import dev.gaurav.notification.api.filter.RequestIdFilter;
import dev.gaurav.notification.api.port.NotificationCommandPort;
import dev.gaurav.notification.api.port.NotificationQueryPort;
import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.DeliveryStatus;
import io.micrometer.tracing.Tracer;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * The HTTP contract of {@link NotificationController}, asserted end to end through the real
 * interceptor chain, the real argument resolver and the real {@link ApiExceptionHandler}.
 *
 * <p>Every test names a production failure rather than a method. Two of them are the reason this
 * class exists at all: an accept path that answers 200 is a lie about delivery, and a cross-tenant
 * read that answers 403 is an existence oracle.
 *
 * <p><strong>Why {@code standaloneSetup} rather than {@code @WebMvcTest}.</strong> Spring Boot 4
 * moved the MVC test slice into a separate {@code spring-boot-webmvc-test} artifact that is not
 * declared for this module, and adding it is not this agent's to do. Standalone setup is not merely
 * a substitute here — the interceptors, the argument resolver and the advice are all wired
 * explicitly below, so a test that passes cannot be passing because some autoconfiguration happened
 * to supply the behaviour. The one thing it does not exercise is the security filter chain, which
 * has its own concerns and no bearing on these assertions.
 */
class NotificationControllerTest {

    private static final UUID REQUEST_ID = UUID.fromString("01998f2a-7c31-7a04-9e12-6f0b3c1d5a88");
    private static final UUID EMAIL_ID = UUID.fromString("01998f2a-7c31-7a04-9e12-6f0b3c1d5a89");
    private static final Instant FIXED_NOW = Instant.parse("2026-08-31T09:14:22.481Z");

    private static final String VALID_BODY = """
            {
              "trafficClass": "TRANSACTIONAL",
              "channels": ["EMAIL"],
              "template": { "code": "order-shipped", "locale": "en-US" },
              "recipients": { "kind": "USER_IDS", "userIds": ["u_9f2a", "u_7b31"] },
              "variables": { "orderId": "A-4821" },
              "schedule": { "type": "IMMEDIATE" },
              "ttlSeconds": 86400
            }""";

    private final NotificationCommandPort commands = mock(NotificationCommandPort.class);
    private final NotificationQueryPort queries = mock(NotificationQueryPort.class);

    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        var clock = Clock.fixed(FIXED_NOW, ZoneOffset.UTC);
        // permitAll = true so the resolver produces the local development caller; the tenant this
        // yields is irrelevant to these assertions, and wiring a full SecurityContext would test
        // Spring Security rather than this controller.
        var security = new ApiSecurityProperties(true, null, Set.of("notifications:send"), null);

        mvc = MockMvcBuilders.standaloneSetup(new NotificationController(commands, queries, clock))
                .setControllerAdvice(new ApiExceptionHandler())
                .addInterceptors(new PayloadSizeInterceptor(), new IdempotencyKeyInterceptor())
                .setCustomArgumentResolvers(new ApiCallerArgumentResolver(security))
                .addFilters(new RequestIdFilter(noTracer()))
                .build();
    }

    @Test
    @DisplayName("a successful send answers 202, never 200 — a 200 would claim delivery already happened")
    void acceptAnswers202() throws Exception {
        when(commands.accept(any(), eq("7f3c1a90-order-4821"), any())).thenReturn(acceptResponse());

        mvc.perform(post("/v1/notifications")
                        .header(IdempotencyKeyInterceptor.HEADER, "7f3c1a90-order-4821")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_BODY))
                .andExpect(status().isAccepted())
                .andExpect(header().string("Location", "/v1/notifications/" + REQUEST_ID))
                .andExpect(jsonPath("$.notificationRequestId").value(REQUEST_ID.toString()))
                .andExpect(jsonPath("$.status").value("ACCEPTED"))
                .andExpect(jsonPath("$.recipientCount").value(2))
                .andExpect(jsonPath("$.notifications[0].channel").value("EMAIL"))
                .andExpect(jsonPath("$.links.status").value("/v1/notifications/" + REQUEST_ID));
    }

    @Test
    @DisplayName("a send with no Idempotency-Key never reaches the use case, so a client retry cannot double-send")
    void missingIdempotencyKeyIsRejectedBeforeTheUseCaseRuns() throws Exception {
        mvc.perform(post("/v1/notifications")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_BODY))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.type").value("https://docs.notification-platform.dev/errors/validation-failed"))
                .andExpect(jsonPath("$.errors[0].field").value(IdempotencyKeyInterceptor.HEADER))
                .andExpect(jsonPath("$.errors[0].code").value("MISSING_IDEMPOTENCY_KEY"));

        // Without this the 400 would be cosmetic: the whole point of the header is that the handler
        // must not run at all.
        verify(commands, never()).accept(any(), any(), any());
    }

    @Test
    @DisplayName("the same key with a different body is a 409, not a silent replay of an unrelated response")
    void reusedKeyWithDifferentFingerprintIsConflict() throws Exception {
        when(commands.accept(any(), any(), any()))
                .thenThrow(new IdempotencyKeyReusedException("7f3c1a90-order-4821", FIXED_NOW));

        mvc.perform(post("/v1/notifications")
                        .header(IdempotencyKeyInterceptor.HEADER, "7f3c1a90-order-4821")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_BODY))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.type")
                        .value("https://docs.notification-platform.dev/errors/idempotency-key-reused"))
                .andExpect(jsonPath("$.errors[0].code").value("FINGERPRINT_MISMATCH"));
    }

    @Test
    @DisplayName("an unknown notification id is a 404 carrying the documented type URI")
    void unknownIdIsNotFound() throws Exception {
        var unknown = UUID.randomUUID();
        when(queries.status(any(), eq(unknown))).thenThrow(new NotificationNotFoundException(unknown));

        mvc.perform(get("/v1/notifications/{id}", unknown))
                .andExpect(status().isNotFound())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.type")
                        .value("https://docs.notification-platform.dev/errors/notification-not-found"));
    }

    @Test
    @DisplayName("reading another tenant's notification is a 404 and never a 403, because a 403 confirms it exists")
    void crossTenantReadIsNotFoundNotForbidden() throws Exception {
        // The query port scopes by tenant inside the predicate, so another tenant's row is simply
        // not found. Pinning 404 here is what stops someone later "improving" this to a 403.
        var otherTenantsId = UUID.randomUUID();
        when(queries.status(any(), eq(otherTenantsId)))
                .thenThrow(new NotificationNotFoundException(otherTenantsId));

        mvc.perform(get("/v1/notifications/{id}", otherTenantsId))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404))
                .andExpect(jsonPath("$.type")
                        .value("https://docs.notification-platform.dev/errors/notification-not-found"));
    }

    @Test
    @DisplayName("every error body is RFC 9457: type, title, status, detail, instance and traceId")
    void errorBodyIsRfc9457() throws Exception {
        when(commands.cancel(any(), any()))
                .thenThrow(new AlreadyDispatchedException(EMAIL_ID, DeliveryStatus.SENDING));

        mvc.perform(post("/v1/notifications/{id}/cancel", EMAIL_ID))
                .andExpect(status().isConflict())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.type")
                        .value("https://docs.notification-platform.dev/errors/already-dispatched"))
                .andExpect(jsonPath("$.title").value("Notification has already been dispatched"))
                .andExpect(jsonPath("$.status").value(409))
                .andExpect(jsonPath("$.detail").isNotEmpty())
                // instance is the URI that failed, not the resource it names — a caller correlating
                // this against their own request log needs the path they actually called.
                .andExpect(jsonPath("$.instance").value("/v1/notifications/" + EMAIL_ID + "/cancel"))
                .andExpect(jsonPath("$.traceId").isNotEmpty());
    }

    @Test
    @DisplayName("an internal failure leaks neither its message nor a stack trace")
    void unexpectedFailureLeaksNothing() throws Exception {
        when(commands.accept(any(), any(), any()))
                .thenThrow(new IllegalStateException("HikariPool-1 connection is not available"));

        mvc.perform(post("/v1/notifications")
                        .header(IdempotencyKeyInterceptor.HEADER, "k-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_BODY))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.type").value("https://docs.notification-platform.dev/errors/internal-error"))
                .andExpect(jsonPath("$.detail").value(Matchers.not(Matchers.containsString("HikariPool"))))
                .andExpect(jsonPath("$.traceId").isNotEmpty())
                .andExpect(jsonPath("$.trace").doesNotExist())
                .andExpect(jsonPath("$.stackTrace").doesNotExist());
    }

    @Test
    @DisplayName("a scheduled sendAt with no UTC offset is a 400 — nine o'clock in an unstated zone is a guess")
    void scheduledSendWithoutOffsetIsRejected() throws Exception {
        var unzoned = """
                {
                  "trafficClass": "TRANSACTIONAL",
                  "channels": ["EMAIL"],
                  "template": { "code": "order-shipped" },
                  "recipients": { "kind": "USER_IDS", "userIds": ["u_9f2a"] },
                  "schedule": { "type": "SCHEDULED", "sendAt": "2026-09-01T09:00:00" }
                }""";

        mvc.perform(post("/v1/notifications")
                        .header(IdempotencyKeyInterceptor.HEADER, "k-2")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(unzoned))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.type").value("https://docs.notification-platform.dev/errors/schedule-invalid"))
                .andExpect(jsonPath("$.errors[0].field").value("schedule.sendAt"))
                .andExpect(jsonPath("$.errors[0].code").value("MISSING_OFFSET"));

        // The key must still be spendable after the caller fixes their timestamp.
        verify(commands, never()).accept(any(), any(), any());
    }

    @Test
    @DisplayName("the same sendAt with an offset is accepted, proving the rejection was about the offset")
    void scheduledSendWithOffsetIsAccepted() throws Exception {
        when(commands.accept(any(), any(), any())).thenReturn(acceptResponse());

        var zoned = """
                {
                  "trafficClass": "TRANSACTIONAL",
                  "channels": ["EMAIL"],
                  "template": { "code": "order-shipped" },
                  "recipients": { "kind": "USER_IDS", "userIds": ["u_9f2a"] },
                  "schedule": { "type": "SCHEDULED", "sendAt": "2026-09-01T09:00:00+05:30" }
                }""";

        mvc.perform(post("/v1/notifications")
                        .header(IdempotencyKeyInterceptor.HEADER, "k-3")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(zoned))
                .andExpect(status().isAccepted());
    }

    @Test
    @DisplayName("a body naming both a template and inline content is rejected, not resolved by precedence")
    void bothTemplateAndContentIsRejected() throws Exception {
        var ambiguous = """
                {
                  "trafficClass": "CRITICAL",
                  "channels": ["SMS"],
                  "template": { "code": "otp" },
                  "content": { "body": "your code is 123456" },
                  "recipients": { "kind": "USER_IDS", "userIds": ["u_9f2a"] }
                }""";

        mvc.perform(post("/v1/notifications")
                        .header(IdempotencyKeyInterceptor.HEADER, "k-4")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(ambiguous))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.type").value("https://docs.notification-platform.dev/errors/validation-failed"));

        verify(commands, never()).accept(any(), any(), any());
    }

    @Test
    @DisplayName("recipients naming USER_IDS while populating an S3 uri is rejected instead of silently picking one")
    void recipientSelectorInconsistentWithItsKindIsRejected() throws Exception {
        var mixed = """
                {
                  "trafficClass": "BULK",
                  "channels": ["EMAIL"],
                  "template": { "code": "newsletter" },
                  "recipients": { "kind": "USER_IDS", "uri": "s3://tenant-bucket/audience.ndjson" }
                }""";

        mvc.perform(post("/v1/notifications")
                        .header(IdempotencyKeyInterceptor.HEADER, "k-5")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mixed))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.type").value("https://docs.notification-platform.dev/errors/validation-failed"));
    }

    @Test
    @DisplayName("a page size above the documented 200 errors rather than silently clamping and hiding rows")
    void oversizedPageLimitIsRejected() throws Exception {
        mvc.perform(get("/v1/notifications/{id}/recipients", EMAIL_ID).param("limit", "5000"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0].field").value("limit"))
                .andExpect(jsonPath("$.errors[0].code").value("OUT_OF_RANGE"));

        verify(queries, never()).recipients(any(), any(), any(), anyInt());
    }

    @Test
    @DisplayName("X-Request-Id supplied by the caller is echoed, so a support ticket can quote one identifier")
    void requestIdIsEchoed() throws Exception {
        when(commands.accept(any(), any(), any())).thenReturn(acceptResponse());

        mvc.perform(post("/v1/notifications")
                        .header(IdempotencyKeyInterceptor.HEADER, "k-6")
                        .header(RequestIdFilter.REQUEST_ID_HEADER, "svc-orders-9931")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_BODY))
                .andExpect(header().string(RequestIdFilter.REQUEST_ID_HEADER, "svc-orders-9931"));
    }

    @Test
    @DisplayName("a newline in X-Request-Id is discarded rather than echoed, so a header cannot forge a log line")
    void requestIdWithControlCharactersIsReplaced() throws Exception {
        when(commands.accept(any(), any(), any())).thenReturn(acceptResponse());

        mvc.perform(post("/v1/notifications")
                        .header(IdempotencyKeyInterceptor.HEADER, "k-7")
                        .header(RequestIdFilter.REQUEST_ID_HEADER, "abc\ndef ERROR fake log line")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_BODY))
                .andExpect(header().string(RequestIdFilter.REQUEST_ID_HEADER,
                        Matchers.matchesPattern("[0-9a-f]{32}")));
    }

    private static AcceptResponse acceptResponse() {
        return new AcceptResponse(
                REQUEST_ID,
                AcceptResponse.ACCEPTED,
                FIXED_NOW,
                2,
                List.of(AcceptResponse.AcceptedNotification.accepted(EMAIL_ID, Channel.EMAIL)),
                new AcceptResponse.Links("/v1/notifications/" + REQUEST_ID));
    }

    /** No tracing in a slice test; correlation must still work, which is the point of the fallback. */
    private static ObjectProvider<Tracer> noTracer() {
        return new ObjectProvider<>() {
            @Override
            public Tracer getObject() {
                throw new NoSuchBeanDefinitionException(Tracer.class);
            }

            @Override
            public Tracer getIfAvailable() {
                return null;
            }
        };
    }
}
