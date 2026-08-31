package dev.gaurav.notification.provider.decorator;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.provider.StubProvider;
import dev.gaurav.notification.provider.spi.SendResult;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The metrics this decorator emits are also the router's scoring inputs, so a tagging mistake here
 * does not just blind a dashboard — it misroutes traffic.
 */
class MeteredProviderTest {

    @Test
    @DisplayName("a rejection is tagged with its failure type, so a provider that starts filtering 4% is visible")
    void rejectionsAreTaggedByFailureType() {
        // The degradation that hurts is not an outage. It is a vendor quietly rejecting a slice of
        // traffic with one specific code, which no error-rate panel separates out.
        var registry = new SimpleMeterRegistry();
        var stub = new StubProvider("mock-sms-primary", Channel.SMS,
                command -> SendResult.Rejected.of(FailureType.UNSUBSCRIBED, "21610",
                        "Attempt to send to unsubscribed recipient", Duration.ofMillis(30)));

        new MeteredProvider(stub, registry).send(StubProvider.command(Channel.SMS, "tok-1"));

        var timer = registry.find(MeteredProvider.SEND_TIMER)
                .tag("provider", "mock-sms-primary")
                .tag("outcome", "rejected")
                .tag("failure", "UNSUBSCRIBED")
                .timer();
        assertThat(timer).isNotNull();
        assertThat(timer.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("an indeterminate outcome is counted apart from a rejection — it is the reconciler's backlog, not an error")
    void indeterminateIsItsOwnOutcome() {
        var registry = new SimpleMeterRegistry();
        var stub = new StubProvider("mock-sms-primary", Channel.SMS,
                command -> new SendResult.Indeterminate(FailureType.PROVIDER_TIMEOUT, "no response",
                        Duration.ofSeconds(5)));

        new MeteredProvider(stub, registry).send(StubProvider.command(Channel.SMS, "tok-2"));

        assertThat(registry.find(MeteredProvider.SEND_TIMER).tag("outcome", "indeterminate").timer())
                .as("folded into 'rejected', the size of the duplicate-risk population becomes unknowable")
                .isNotNull();
        assertThat(registry.find(MeteredProvider.SEND_TIMER).tag("outcome", "rejected").timer()).isNull();
    }

    @Test
    @DisplayName("spend is accumulated in micros, so a failover to a pricier vendor shows up before the invoice does")
    void costIsCounted() {
        var registry = new SimpleMeterRegistry();
        var stub = new StubProvider("mock-sms-secondary", Channel.SMS,
                command -> new SendResult.Accepted("SM1", Duration.ofMillis(40), 9_500L));
        var metered = new MeteredProvider(stub, registry);

        metered.send(StubProvider.command(Channel.SMS, "tok-3"));
        metered.send(StubProvider.command(Channel.SMS, "tok-4"));

        assertThat(registry.find(MeteredProvider.COST_COUNTER).counter().count()).isEqualTo(19_000.0);
    }

    @Test
    @DisplayName("a Retry-After we were given is counted, so 'we honoured backoff' is a fact and not a claim")
    void retryAfterIsCounted() {
        var registry = new SimpleMeterRegistry();
        var stub = new StubProvider("mock-push-primary", Channel.PUSH,
                command -> new SendResult.Rejected(FailureType.RATE_LIMITED, "QUOTA_EXCEEDED",
                        "429", Duration.ofMillis(15), Optional.of(Duration.ofSeconds(60))));

        new MeteredProvider(stub, registry).send(StubProvider.command(Channel.PUSH, "tok-5"));

        assertThat(registry.find(MeteredProvider.RETRY_AFTER_COUNTER).counter().count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("an adapter that throws is recorded as outcome=error and still rethrown, so the bug cannot hide")
    void adapterBugsAreRecordedThenRethrown() {
        var registry = new SimpleMeterRegistry();
        var stub = new StubProvider("mock-sms-primary", Channel.SMS, command -> {
            throw new IllegalStateException("mapping gap");
        });

        assertThatThrownBy(() -> new MeteredProvider(stub, registry).send(StubProvider.command(Channel.SMS, "tok-6")))
                .isInstanceOf(IllegalStateException.class);

        assertThat(registry.find(MeteredProvider.SEND_TIMER).tag("outcome", "error").timer()).isNotNull();
    }

    @Test
    @DisplayName("tag cardinality stays bounded — the vendor's free-text message is never a tag")
    void theVendorMessageIsNotATag() {
        // A provider that echoes the recipient address in its error text would otherwise mint one
        // time series per user, which is how a metrics backend falls over.
        var registry = new SimpleMeterRegistry();
        var metered = new MeteredProvider(new StubProvider("mock-sms-primary", Channel.SMS,
                command -> SendResult.Rejected.of(FailureType.INVALID_RECIPIENT, "21614",
                        "'To' number " + command.address() + " is not valid", Duration.ofMillis(12))),
                registry);

        for (var i = 0; i < 50; i++) {
            metered.send(StubProvider.command(Channel.SMS, "tok-" + i));
        }

        assertThat(registry.find(MeteredProvider.SEND_TIMER).timers())
                .as("50 sends with 50 distinct messages must still be one time series")
                .hasSize(1);
    }
}
