package dev.gaurav.notification.provider.spi;

import dev.gaurav.notification.domain.enums.FailureType;

import java.time.Duration;
import java.util.Optional;

/**
 * The outcome of one provider call.
 *
 * <p>Sealed with <strong>three</strong> cases, not two. {@link Indeterminate} is what forces every
 * caller to handle "the provider may or may not have delivered" at compile time — the case that
 * produces duplicate one-time passcodes when it is collapsed into failure.
 */
public sealed interface SendResult {

    /** The provider acknowledged the message. */
    record Accepted(String providerMessageId, Duration latency, long costMicros) implements SendResult {}

    /** The provider refused it, and we know why. */
    record Rejected(FailureType type, String code, String message, Duration latency,
                    Optional<Duration> retryAfter) implements SendResult {
        public static Rejected of(FailureType type, String code, String message, Duration latency) {
            return new Rejected(type, code, message, latency, Optional.empty());
        }
    }

    /** Timed out or connection lost after the request was sent. Outcome genuinely unknown. */
    record Indeterminate(FailureType type, String message, Duration latency) implements SendResult {}

    default boolean isSuccess() { return this instanceof Accepted; }
}
