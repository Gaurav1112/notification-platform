package dev.gaurav.notification.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import dev.gaurav.notification.domain.enums.AttemptState;
import dev.gaurav.notification.domain.enums.FailureType;

import java.time.Instant;
import java.util.List;

/**
 * Every provider call made for a notification, in order.
 *
 * <p>This is the endpoint that answers "you say it failed — what actually happened", and it is
 * exposed to callers rather than kept in logs because the alternative is a support engineer with a
 * database session. The attempt ledger is written <em>before</em> each provider call, so a worker
 * that dies mid-send still leaves a row here.
 */
public record AttemptListResponse(List<AttemptView> items) {

    /**
     * One attempt.
     *
     * <p><strong>{@code state: "UNKNOWN"} is a first-class documented outcome, not a bug.</strong>
     * It means the request reached the provider and the response did not reach us — the provider
     * may or may not have delivered. Collapsing it into FAILED and retrying is how a user receives
     * three one-time passcodes; collapsing it into SUCCEEDED is how a password reset silently never
     * arrives. It resolves later by webhook or reconciliation, and until then the honest value is
     * UNKNOWN.
     *
     * <p>{@code provider} changes between attempts when failover fires — reading two different
     * codes down this list is the failover story, visible without a trace backend.
     *
     * @param costMicros populated only on success; a failed call still costs latency but not money
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record AttemptView(
            int attemptNumber,
            String provider,
            AttemptState state,
            FailureType failureType,
            String providerMessageId,
            Long latencyMs,
            Long costMicros,
            Instant startedAt,
            Instant retryScheduledAt
    ) {
    }
}
