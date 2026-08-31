package dev.gaurav.notification.api.error;

import java.time.Duration;
import java.util.List;

/**
 * A concurrent request holds this {@code Idempotency-Key} and has not yet stored its response.
 *
 * <p><strong>Why this is not simply "wait for the other one":</strong> blocking would hold a Tomcat
 * thread and a Postgres connection for the duration of somebody else's request, which is how a
 * client-side double-submit storm turns into pool exhaustion on a path with a 250 ms p99 budget.
 * Answering 409 immediately keeps the failure local to the duplicate.
 *
 * <p>It is also not a 202. A 202 would promise the caller a notification id they cannot get, since
 * the winning request owns the response body. Carrying {@code Retry-After} tells them the honest
 * thing: come back in a moment and you will get the replay.
 */
public class RequestInProgressException extends ApiException {

    private static final Duration SUGGESTED_RETRY = Duration.ofSeconds(1);

    public RequestInProgressException(String key) {
        super(ProblemType.REQUEST_IN_PROGRESS,
                "Another request is already processing key '%s'.".formatted(key),
                List.of(FieldViolation.of("Idempotency-Key", "CLAIM_HELD")),
                SUGGESTED_RETRY);
    }
}
