package dev.gaurav.notification.provider;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.TrafficClass;
import dev.gaurav.notification.provider.spi.NotificationProvider;
import dev.gaurav.notification.provider.spi.ProviderCapabilities;
import dev.gaurav.notification.provider.spi.ProviderCode;
import dev.gaurav.notification.provider.spi.SendCommand;
import dev.gaurav.notification.provider.spi.SendResult;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

/**
 * A provider whose behaviour a test dictates outright.
 *
 * <p>Used instead of Mockito wherever the test is about <em>how many times</em> the delegate was
 * called, because a recorded call list reads better in a failure message than a verify() stack
 * trace, and because these stubs are handed to real thread pools.
 */
public final class StubProvider implements NotificationProvider {

    private final ProviderCode code;
    private final Channel channel;
    private final Function<SendCommand, SendResult> behaviour;
    private final List<SendCommand> received = new CopyOnWriteArrayList<>();
    private volatile boolean healthy = true;

    public StubProvider(String code, Channel channel, Function<SendCommand, SendResult> behaviour) {
        this.code = ProviderCode.of(code);
        this.channel = channel;
        this.behaviour = behaviour;
    }

    public static StubProvider alwaysAccepts(String code, Channel channel) {
        return new StubProvider(code, channel,
                command -> new SendResult.Accepted("id-" + command.idempotencyToken(),
                        Duration.ofMillis(10), 100L));
    }

    public static SendCommand command(Channel channel, String token) {
        return new SendCommand(UUID.randomUUID(), channel, TrafficClass.TRANSACTIONAL,
                "+14155550123", null, "body", token, Map.of(), Duration.ofSeconds(5));
    }

    public StubProvider unhealthy() {
        this.healthy = false;
        return this;
    }

    public List<SendCommand> received() {
        return List.copyOf(received);
    }

    public int callCount() {
        return received.size();
    }

    @Override
    public Channel channel() {
        return channel;
    }

    @Override
    public ProviderCode code() {
        return code;
    }

    @Override
    public ProviderCapabilities capabilities() {
        return ProviderCapabilities.singleSend(true);
    }

    @Override
    public SendResult send(SendCommand command) {
        received.add(command);
        return behaviour.apply(command);
    }

    @Override
    public boolean isHealthy() {
        return healthy;
    }
}
