package dev.gaurav.notification.worker.status;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.SuppressionReason;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Records the suppression as a log line and a counter, because there is nowhere else to put it yet.
 *
 * <p>TODO(phase-9): {@code suppression_entry} is not in {@code V1__baseline.sql}. It needs its own
 * table rather than a column on {@code notification_recipient}, for three reasons that are worth
 * stating before someone takes the shortcut: the suppression is a property of the <em>address</em>
 * and not of one message; it has to be readable at fan-out time, when no recipient row exists yet;
 * and it must survive user erasure, which deletes recipient rows.
 *
 * <p>Until then this is a visible gap rather than a silent one. The counter is tagged by reason, so
 * {@code notification_suppression_pending_total{reason="HARD_BOUNCE"}} climbing is the evidence
 * that the table is needed, and how much it is needed.
 */
public class LoggingSuppressionWriter implements SuppressionWriter {

    private static final Logger log = LoggerFactory.getLogger(LoggingSuppressionWriter.class);

    private final MeterRegistry meters;

    public LoggingSuppressionWriter(MeterRegistry meters) {
        this.meters = meters;
        log.warn("no suppression store configured: hard bounces and unsubscribes are counted and "
                + "logged but NOT persisted; the same address can be mailed again");
    }

    @Override
    public void suppress(long tenantId, UUID recipientId, Channel channel, SuppressionReason reason) {
        Counter.builder("notification.suppression.pending")
                .description("suppressions that could not be persisted because no store exists yet")
                .tag("channel", channel.name())
                .tag("reason", reason.name())
                .register(meters)
                .increment();
        log.warn("suppression not persisted: tenant={} recipient={} channel={} reason={}",
                tenantId, recipientId, channel, reason);
    }
}
