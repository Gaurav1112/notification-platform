package dev.gaurav.notification.worker.orchestrator;

import dev.gaurav.notification.messaging.event.NotificationRequestedEvent;

import java.util.List;

/**
 * Resolves the recipient list of a request, whether it travelled inline or as a claim check.
 *
 * <p>Above {@link NotificationRequestedEvent#MAX_INLINE_RECIPIENTS} the list is written to S3 and
 * only the URI rides on the event: a 10M-recipient blast inlined would be a ~600 MB Kafka record
 * against a 1 MB broker default, and raising {@code max.message.bytes} globally to accommodate one
 * workload degrades latency on every other topic.
 *
 * <p>A port because the S3 reader belongs to the deployment, not to the orchestrator, and because
 * the interesting test — a manifest that is missing or truncated — must be writable without an
 * object store.
 */
public interface RecipientManifestReader {

    /**
     * The user references for a request, in manifest order.
     *
     * <p>Returned eagerly as a list rather than a stream on purpose at this stage: fan-out writes
     * one database row per recipient per channel, so a genuinely 10M-row manifest needs chunked
     * expansion with progress tracking, not a lazier iterator over the same unbounded work.
     *
     * @throws UnsupportedOperationException when the request is a claim check and no reader is
     *         configured — deliberately loud, because silently fanning out zero recipients would
     *         report a campaign as complete having sent nothing
     */
    List<String> read(NotificationRequestedEvent event);
}
