package dev.gaurav.notification.provider.registry;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.provider.StubProvider;
import dev.gaurav.notification.provider.spi.NotificationProvider;
import dev.gaurav.notification.provider.spi.ProviderCode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProviderRegistryTest {

    @Test
    @DisplayName("two providers under the same code fail the context start instead of silently shadowing each other")
    void duplicateCodesAreFatal() {
        // Otherwise the chaos endpoint injects a fault into one bean while the router keeps using
        // the other, and the failover demo appears to do nothing at all.
        var first = StubProvider.alwaysAccepts("mock-sms-primary", Channel.SMS);
        var second = StubProvider.alwaysAccepts("mock-sms-primary", Channel.SMS);

        assertThatThrownBy(() -> new ProviderRegistry(List.of(first, second)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("mock-sms-primary");
    }

    @Test
    @DisplayName("a channel with no provider returns an empty list, not null — nobody guards a lookup twice")
    void anUnservedChannelIsEmptyNotNull() {
        var registry = new ProviderRegistry(List.of(StubProvider.alwaysAccepts("mock-sms-primary", Channel.SMS)));

        assertThat(registry.forChannel(Channel.PUSH)).isEmpty();
    }

    @Test
    @DisplayName("providers are grouped by channel, so an email never gets routed to an SMS adapter")
    void providersAreIndexedByChannel() {
        var registry = new ProviderRegistry(List.of(
                StubProvider.alwaysAccepts("mock-sms-primary", Channel.SMS),
                StubProvider.alwaysAccepts("mock-sms-secondary", Channel.SMS),
                StubProvider.alwaysAccepts("mock-email-primary", Channel.EMAIL)));

        assertThat(registry.forChannel(Channel.SMS)).hasSize(2);
        assertThat(registry.forChannel(Channel.EMAIL))
                .extracting(NotificationProvider::code)
                .containsExactly(ProviderCode.of("mock-email-primary"));
    }

    @Test
    @DisplayName("lookup by code resolves a stored attempt row and an inbound webhook back to a live adapter")
    void lookupByCode() {
        var registry = new ProviderRegistry(List.of(
                StubProvider.alwaysAccepts("mock-sms-primary", Channel.SMS),
                StubProvider.alwaysAccepts("mock-push-primary", Channel.PUSH)));

        assertThat(registry.byCode(ProviderCode.of("mock-push-primary")))
                .map(NotificationProvider::channel)
                .contains(Channel.PUSH);
        assertThat(registry.byCode(ProviderCode.of("twilio-live")))
                .as("an unknown code must be an empty Optional, not an exception — a stale webhook is normal")
                .isEmpty();
    }

    @Test
    @DisplayName("bean-definition order is preserved, so the operator's stated primary really is listed first")
    void channelOrderIsStable() {
        var registry = new ProviderRegistry(List.of(
                StubProvider.alwaysAccepts("mock-sms-primary", Channel.SMS),
                StubProvider.alwaysAccepts("mock-sms-secondary", Channel.SMS)));

        assertThat(registry.forChannel(Channel.SMS))
                .extracting(p -> p.code().value())
                .containsExactly("mock-sms-primary", "mock-sms-secondary");
    }

    @Test
    @DisplayName("an empty deployment starts rather than crashing, because a channel-less config is an ops problem not a bug")
    void anEmptyRegistryIsLegal() {
        var registry = new ProviderRegistry(List.of());

        assertThat(registry.all()).isEmpty();
        assertThat(registry.codes()).isEmpty();
        assertThat(registry.forChannel(Channel.SMS)).isEmpty();
    }
}
