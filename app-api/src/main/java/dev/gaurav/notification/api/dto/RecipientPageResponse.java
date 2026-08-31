package dev.gaurav.notification.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import dev.gaurav.notification.domain.enums.DeliveryStatus;
import dev.gaurav.notification.domain.enums.SuppressionReason;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * One cursor page of per-recipient status.
 *
 * <p><strong>Cursor, never offset.</strong> {@code notification_recipient} is RANGE-partitioned by
 * day, so {@code OFFSET 200000} makes PostgreSQL walk and discard two hundred thousand rows across
 * every partition the planner cannot prune — the query gets slower the further a caller paginates,
 * which is precisely backwards from what they need when reconciling a ten-million-recipient
 * campaign. A keyset cursor is O(1) per page regardless of depth.
 *
 * <p>{@code nextCursor} is serialised even when null, because "the field is absent" and "there are
 * no more pages" must not look the same to a client loop.
 */
public record RecipientPageResponse(List<RecipientView> items, String nextCursor) {

    /**
     * One recipient's outcome.
     *
     * <p><strong>{@code addressHint}, never the address.</strong> This endpoint is reachable by any
     * token with {@code notifications:read}, and a status API that returns plaintext e-mail
     * addresses and phone numbers is a bulk PII export with pagination built in. {@code g***@example.com}
     * is enough for a human to recognise the row they are looking for and useless for harvesting.
     * The same reasoning is why addresses are field-encrypted at rest and never logged.
     *
     * <p>{@code suppressionReason} is present only on a {@code SUPPRESSED} row — the reason is the
     * answer to "why didn't my user get this", which is the single most common support question
     * this API exists to answer without a database session.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record RecipientView(
            UUID recipientId,
            String addressHint,
            DeliveryStatus status,
            Instant deliveredAt,
            Integer attemptCount,
            SuppressionReason suppressionReason
    ) {
    }
}
