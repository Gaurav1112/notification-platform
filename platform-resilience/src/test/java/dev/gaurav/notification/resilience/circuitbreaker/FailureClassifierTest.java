package dev.gaurav.notification.resilience.circuitbreaker;

import dev.gaurav.notification.domain.enums.FailureType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/** The 4xx rule, and the exceptions to it that are deliberate rather than accidental. */
class FailureClassifierTest {

    @ParameterizedTest
    @ValueSource(ints = {400, 404, 409, 413, 422, 451})
    @DisplayName("a bad campaign full of malformed payloads must not take a healthy provider offline")
    void clientErrorsNeverTripTheBreaker(int status) {
        FailureType type = FailureClassifier.classify(status);

        assertThat(FailureClassifier.shouldRecordAsCircuitFailure(type))
                .as("HTTP %d is our bug; counting it drives the failure rate to 100%% and opens "
                        + "the circuit on a provider that is answering correctly", status)
                .isFalse();
    }

    @Test
    @DisplayName("exceeding our own quota (429) must not fence off a provider that is working fine")
    void rateLimitingIsThrottledNotBroken() {
        assertThat(FailureClassifier.classify(429)).isEqualTo(FailureType.RATE_LIMITED);
        assertThat(FailureClassifier.shouldRecordAsCircuitFailure(FailureType.RATE_LIMITED))
                .as("the token bucket and immediate failover are the control loop for 429, "
                        + "not a 30-second channel outage")
                .isFalse();
    }

    @ParameterizedTest
    @ValueSource(ints = {401, 403})
    @DisplayName("revoked credentials are the deliberate 4xx exception — every call will fail, so open")
    void authFailuresDoTripTheBreaker(int status) {
        assertThat(FailureClassifier.classify(status)).isEqualTo(FailureType.AUTH_FAILURE);
        assertThat(FailureClassifier.shouldRecordAsCircuitFailure(FailureType.AUTH_FAILURE))
                .as("account-scoped, not message-scoped: a well-formed request would fail too")
                .isTrue();
    }

    @ParameterizedTest
    @ValueSource(ints = {500, 502, 503})
    @DisplayName("a 5xx is the provider's fault and is exactly what the breaker exists to count")
    void serverErrorsTripTheBreaker(int status) {
        assertThat(FailureClassifier.classify(status)).isEqualTo(FailureType.PROVIDER_5XX);
        assertThat(FailureClassifier.shouldRecordAsCircuitFailure(FailureType.PROVIDER_5XX)).isTrue();
    }

    @Test
    @DisplayName("504 is a timeout, not a 5xx — the message may already have been sent")
    void gatewayTimeoutIsIndeterminate() {
        FailureType type = FailureClassifier.classify(504);

        assertThat(type).isEqualTo(FailureType.PROVIDER_TIMEOUT);
        assertThat(type.isOutcomeIndeterminate())
                .as("classifying 504 as PROVIDER_5XX would licence a blind retry and a second OTP")
                .isTrue();
    }

    @Test
    @DisplayName("a socket timeout wrapped three layers deep is still a timeout, not a generic IO error")
    void unwrapsTheCauseChainToFindTheTimeout() {
        var wrapped = new IllegalStateException("send failed",
                new UncheckedIOException(new SocketTimeoutException("read timed out")));

        FailureType type = FailureClassifier.classify(wrapped);

        assertThat(type).isEqualTo(FailureType.PROVIDER_TIMEOUT);
        assertThat(type.isOutcomeIndeterminate()).isTrue();
    }

    @Test
    @DisplayName("a connection that was never established definitely did not deliver anything")
    void connectFailuresAreSafeToRetry() {
        assertThat(FailureClassifier.classify(new ConnectException("refused")))
                .isEqualTo(FailureType.TRANSIENT_NETWORK);
        assertThat(FailureClassifier.classify(new UnknownHostException("api.example.com")))
                .isEqualTo(FailureType.TRANSIENT_NETWORK);
        assertThat(FailureType.TRANSIENT_NETWORK.isOutcomeIndeterminate()).isFalse();
    }

    @Test
    @DisplayName("a NullPointerException in our adapter must not be reported as a provider outage")
    void unrecognisedExceptionsAreOurBug() {
        FailureType type = FailureClassifier.classify(new NullPointerException("template was null"));

        assertThat(type).isEqualTo(FailureType.PERMANENT_UNKNOWN);
        assertThat(FailureClassifier.shouldRecordAsCircuitFailure(type))
                .as("defaulting unknown exceptions to retryable makes every code bug open a circuit")
                .isFalse();
    }

    @Test
    @DisplayName("a client that rethrows its own exception as its own cause does not hang the classifier")
    void selfReferentialCauseTerminates() {
        var selfReferential = new RuntimeException("wrapped by a buggy client") {
            @Override
            public synchronized Throwable getCause() {
                return this;
            }
        };

        assertThat(FailureClassifier.classify(selfReferential)).isEqualTo(FailureType.PERMANENT_UNKNOWN);
    }

    @Test
    @DisplayName("a 2xx cannot be classified as a failure — silently mapping it would poison the window")
    void successStatusIsRejected() {
        assertThatIllegalArgumentException().isThrownBy(() -> FailureClassifier.classify(202));
    }
}
