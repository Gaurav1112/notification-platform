package dev.gaurav.notification.resilience.circuitbreaker;

import dev.gaurav.notification.domain.enums.FailureType;

import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.net.http.HttpTimeoutException;
import java.util.concurrent.TimeoutException;
import javax.net.ssl.SSLException;

/**
 * Turns whatever a provider adapter threw or returned into a {@link FailureType}, and decides
 * whether that failure is evidence about the <em>provider's health</em> or about <em>our own
 * request</em>.
 *
 * <p><strong>The failure this prevents: a 4xx must never trip the breaker.</strong> An HTTP 400
 * because a template rendered a malformed E.164 number is our bug. The provider answered correctly
 * and quickly; it is completely healthy. Feeding that into the circuit breaker's failure rate means
 * one bad campaign — every message in it malformed the same way — drives the failure rate to 100%,
 * opens the circuit, and takes a perfectly working provider offline for every other tenant on the
 * platform. The blast radius of a content bug becomes a channel-wide outage, and the metric that
 * should have told the on-call engineer "your payloads are wrong" instead says "Twilio is down".
 *
 * <p>The rule the classifier encodes: a circuit breaker exists to stop calls that <em>cannot</em>
 * succeed because the remote side is broken. If a different, well-formed request would have
 * succeeded right now, the failure is ours and must not count.
 *
 * <p>Static, because classification is a pure function of the failure and must give the same answer
 * in the worker, in the reconciler and in a test.
 */
public final class FailureClassifier {

    private FailureClassifier() {
    }

    /**
     * Maps an HTTP status returned by a provider to a failure type.
     *
     * @throws IllegalArgumentException for a status below 400 — a success cannot be classified, and
     *                                  silently mapping one to a failure type would let a bug in a
     *                                  caller quietly poison the breaker's window
     */
    public static FailureType classify(int httpStatus) {
        if (httpStatus < 400) {
            throw new IllegalArgumentException("not a failure status: " + httpStatus);
        }
        return switch (httpStatus) {
            case 401, 403 -> FailureType.AUTH_FAILURE;
            case 402 -> FailureType.QUOTA_EXCEEDED;
            // 404 and 422 from a send endpoint mean the address, not the URL, was rejected.
            case 404, 422 -> FailureType.INVALID_RECIPIENT;
            case 408 -> FailureType.PROVIDER_TIMEOUT;
            case 413 -> FailureType.PAYLOAD_TOO_LARGE;
            case 429 -> FailureType.RATE_LIMITED;
            // 504 is not PROVIDER_5XX: a gateway timeout means the request may have reached the
            // provider and been acted on, so the outcome is indeterminate and a blind retry can
            // send a second message.
            case 504 -> FailureType.PROVIDER_TIMEOUT;
            default -> httpStatus >= 500 ? FailureType.PROVIDER_5XX : FailureType.PERMANENT_UNKNOWN;
        };
    }

    /**
     * Maps a thrown exception to a failure type, walking the cause chain because HTTP clients wrap
     * socket failures several layers deep and only the root cause distinguishes "never connected"
     * from "connected, then timed out" — which is the difference between a safe retry and a
     * duplicate one-time passcode.
     */
    public static FailureType classify(Throwable throwable) {
        for (Throwable t = throwable; t != null; t = t.getCause() == t ? null : t.getCause()) {
            // Timeouts first: SocketTimeoutException extends IOException, and the order in which
            // these are tested is the whole distinction between TRANSIENT_NETWORK (definitely not
            // delivered) and PROVIDER_TIMEOUT (possibly delivered).
            if (t instanceof SocketTimeoutException
                    || t instanceof HttpTimeoutException
                    || t instanceof TimeoutException) {
                return FailureType.PROVIDER_TIMEOUT;
            }
            if (t instanceof ConnectException
                    || t instanceof UnknownHostException
                    || t instanceof NoRouteToHostException
                    || t instanceof SSLException) {
                // The request never reached the provider, so it definitely was not delivered.
                return FailureType.TRANSIENT_NETWORK;
            }
            if (t instanceof IOException) {
                return FailureType.TRANSIENT_NETWORK;
            }
        }
        // An unrecognised exception is our bug until proven otherwise. Defaulting to a retryable
        // type here would make every NullPointerException in an adapter look like a provider
        // outage, and would open the breaker on it.
        return FailureType.PERMANENT_UNKNOWN;
    }

    /**
     * Whether this failure is evidence that the provider itself is unhealthy, and therefore belongs
     * in the circuit breaker's sliding window.
     *
     * <p>{@code false} for every permanent, message-scoped failure — invalid recipients, oversized
     * payloads, template errors, content rejections. Those are our bugs; see the class Javadoc.
     *
     * <p>{@code false} for {@link FailureType#RATE_LIMITED} as well, even though 429 is transient.
     * A 429 says <em>we</em> are sending too fast, not that the provider is broken; the correct
     * control loop is the token bucket and immediate failover, not the breaker. Recording it would
     * mean that exceeding a quota by one call takes the entire provider offline for 30 seconds,
     * turning a throttle into an outage.
     *
     * <p>{@code true} for {@link FailureType#AUTH_FAILURE} and {@link FailureType#QUOTA_EXCEEDED}
     * despite both arriving as 4xx. These are the exception that proves the rule: they are
     * account-scoped, not message-scoped. Every call we make with revoked credentials will fail, so
     * opening the circuit is exactly right — it stops thousands of doomed calls per second while
     * the page fires.
     */
    public static boolean shouldRecordAsCircuitFailure(FailureType type) {
        return switch (type) {
            case TRANSIENT_NETWORK, PROVIDER_TIMEOUT, PROVIDER_5XX,
                 AUTH_FAILURE, QUOTA_EXCEEDED -> true;
            case RATE_LIMITED,
                 INVALID_RECIPIENT, DEVICE_UNREGISTERED, UNSUBSCRIBED, CONTENT_REJECTED,
                 TEMPLATE_ERROR, PAYLOAD_TOO_LARGE, PERMANENT_UNKNOWN -> false;
        };
    }

    /** Convenience for the breaker's {@code recordException} predicate. */
    public static boolean shouldRecordAsCircuitFailure(Throwable throwable) {
        return shouldRecordAsCircuitFailure(classify(throwable));
    }
}
