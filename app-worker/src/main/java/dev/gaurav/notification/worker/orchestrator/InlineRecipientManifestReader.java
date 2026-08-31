package dev.gaurav.notification.worker.orchestrator;

import dev.gaurav.notification.messaging.event.NotificationRequestedEvent;

import java.util.List;

/**
 * Serves inline recipient lists and refuses claim checks.
 *
 * <p>TODO(phase-9): the S3-backed reader.
 *
 * <p>Refusing rather than returning an empty list is the whole design of this stub. An empty list
 * fans out zero recipients, marks the request expanded, and reports a 10M-recipient campaign as
 * successfully completed with a delivery count of zero — a failure that looks exactly like
 * success on every dashboard. The exception routes the record to the DLQ with the manifest URI
 * attached, which is a bug report someone will actually read.
 */
public class InlineRecipientManifestReader implements RecipientManifestReader {

    @Override
    public List<String> read(NotificationRequestedEvent event) {
        if (event.isClaimCheck()) {
            throw new UnsupportedOperationException(
                    "no RecipientManifestReader configured for claim check " + event.recipientRef());
        }
        return event.recipientIds();
    }
}
