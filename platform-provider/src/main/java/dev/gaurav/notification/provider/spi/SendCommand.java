package dev.gaurav.notification.provider.spi;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.TrafficClass;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * One send instruction. Immutable, so it is safe to hand to a decorator chain and to retry.
 *
 * @param recipientId       our identifier, used as the correlation reference
 * @param idempotencyToken  passed to the provider where supported, and always recorded on the
 *                          attempt row so a redelivery cannot create a second attempt
 * @param address           the destination (E.164, email address or device token)
 * @param deadline          hard upper bound; must stay well below the Kafka poll interval
 */
public record SendCommand(
        UUID recipientId,
        Channel channel,
        TrafficClass trafficClass,
        String address,
        String subject,
        String body,
        String idempotencyToken,
        Map<String, String> attributes,
        Duration deadline) {

    public SendCommand {
        Objects.requireNonNull(recipientId, "recipientId");
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(address, "address");
        Objects.requireNonNull(idempotencyToken, "idempotencyToken");
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
        deadline = deadline == null ? Duration.ofSeconds(5) : deadline;
    }
}
