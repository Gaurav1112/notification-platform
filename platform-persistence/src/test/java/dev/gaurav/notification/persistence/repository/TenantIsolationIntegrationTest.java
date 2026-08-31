package dev.gaurav.notification.persistence.repository;

import static org.assertj.core.api.Assertions.assertThat;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.DeliveryStatus;
import dev.gaurav.notification.domain.enums.ScheduleType;
import dev.gaurav.notification.domain.enums.TrafficClass;
import dev.gaurav.notification.persistence.AbstractPostgresTest;
import dev.gaurav.notification.persistence.entity.DeliveryAttempt;
import dev.gaurav.notification.persistence.entity.NotificationEntity;
import dev.gaurav.notification.persistence.entity.NotificationEvent;
import dev.gaurav.notification.persistence.entity.NotificationRecipient;
import dev.gaurav.notification.persistence.entity.NotificationRequest;
import dev.gaurav.notification.persistence.entity.Tenant;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Two tenants in one database, and every tenant-scoped finder read as the wrong one.
 *
 * <p><strong>Why this exists alongside {@link TenantScopedQueryArchTest}.</strong> That test reads
 * the query text and proves the predicate is written. This one proves the predicate <em>works</em>
 * against a real PostgreSQL with real rows — that the column it names is the column that carries
 * the tenant, that the parameter binds, and that the answer for another tenant's identifier is
 * empty rather than an exception, a partial row, or a row. Those are different failures and the
 * static check cannot see any of them: a predicate on the wrong column reads perfectly.
 *
 * <p>Every assertion is paired. The negative alone would pass if the seed silently wrote nothing —
 * which is the way this exact test usually rots — so each finder is also asked for the row it
 * <em>should</em> return, as its own tenant, in the same test.
 *
 * <p>The two writes matter more than the reads and are pinned separately. A cross-tenant read
 * leaks; a cross-tenant status transition <em>destroys</em>, because the guard is monotonic and a
 * status that lands cannot be walked back — the genuine provider callback that follows is then
 * refused as post-terminal and the recipient is never contacted again.
 */
class TenantIsolationIntegrationTest extends AbstractPostgresTest {

    @Autowired
    private NotificationRepository notifications;

    @Autowired
    private NotificationRecipientRepository recipients;

    @Autowired
    private NotificationRequestRepository requests;

    @Autowired
    private NotificationEventRepository events;

    @Autowired
    private DeliveryAttemptRepository attempts;

    @Autowired
    private IdempotencyRepository idempotency;

    @Autowired
    private TenantRepository tenants;

    private Instant now;
    private Instant windowFrom;
    private Instant windowTo;

    private Fixture one;
    private Fixture two;

    /**
     * One tenant's whole vertical slice: request, notification, recipient, event, attempt and
     * idempotency key. Held as a record so a test can name {@code two.recipientId} and read as
     * {@code one.tenantId} without either being a magic number.
     */
    private record Fixture(long tenantId,
                           UUID requestId,
                           UUID notificationId,
                           UUID recipientId,
                           String idempotencyKey,
                           String providerMessageId,
                           String attemptToken) {
    }

    @BeforeEach
    void seedTwoTenantsWithIdenticalShapes() {
        now = Instant.now();
        windowFrom = now.truncatedTo(ChronoUnit.DAYS);
        windowTo = windowFrom.plus(Duration.ofDays(1));

        one = seed("acme");
        two = seed("globex");

        // The whole test is meaningless if both slices landed under the same tenant.
        assertThat(one.tenantId()).isNotEqualTo(two.tenantId());
    }

    @Test
    @DisplayName("a notification lookup with another tenant's id returns empty, not that tenant's row")
    void notificationLookupIsScopedToItsTenant() {
        assertThat(notifications.findInWindow(two.notificationId(), one.tenantId(), windowFrom, windowTo))
                .as("tenant one asking for tenant two's notification must see nothing")
                .isEmpty();

        assertThat(notifications.findInWindow(two.notificationId(), two.tenantId(), windowFrom, windowTo))
                .as("and the row really is there, so the emptiness above is the predicate and not a "
                        + "failed seed")
                .isPresent();
    }

    @Test
    @DisplayName("a recipient lookup with another tenant's id returns empty, so the address ciphertext stays put")
    void recipientLookupIsScopedToItsTenant() {
        assertThat(recipients.findInWindow(two.recipientId(), one.tenantId(), windowFrom, windowTo))
                .isEmpty();
        assertThat(recipients.findInWindow(two.recipientId(), two.tenantId(), windowFrom, windowTo))
                .isPresent();
    }

    @Test
    @DisplayName("walking another tenant's notification to its recipients enumerates nothing")
    void recipientsByNotificationAreScopedToTheirTenant() {
        assertThat(recipients.findByNotification(two.notificationId(), one.tenantId(), windowFrom, windowTo))
                .as("one guessed notification id must not become that campaign's audience list")
                .isEmpty();
        assertThat(recipients.findByNotification(two.notificationId(), two.tenantId(), windowFrom, windowTo))
                .hasSize(1);
    }

    @Test
    @DisplayName("when two tenants share a provider_message_id, each resolves only to its own recipient")
    void providerMessageLookupIsScopedToItsTenant() {
        // Both fixtures carry the *same* provider_message_id on purpose. It is the vendor's string,
        // it arrives on a public endpoint, and two tenants on one vendor account collide on it
        // routinely — the column has no unique index precisely because we do not control it. So
        // "returns nothing" is the wrong assertion here; the right one is that a webhook processed
        // under tenant one can never reach tenant two's row, which is the transition that would
        // otherwise be applied to a stranger's message.
        var seenByOne = recipients.findByProviderMessageId(
                one.providerMessageId(), one.tenantId(), windowFrom, windowTo);
        var seenByTwo = recipients.findByProviderMessageId(
                two.providerMessageId(), two.tenantId(), windowFrom, windowTo);

        assertThat(one.providerMessageId()).isEqualTo(two.providerMessageId());
        assertThat(seenByOne).extracting(NotificationRecipient::getId)
                .containsExactly(one.recipientId());
        assertThat(seenByTwo).extracting(NotificationRecipient::getId)
                .containsExactly(two.recipientId());
    }

    @Test
    @DisplayName("the attempt ledger for another tenant's recipient is empty, so cost and provider mix do not leak")
    void attemptHistoryIsScopedToItsTenant() {
        assertThat(attempts.findByRecipient(two.recipientId(), one.tenantId(), windowFrom, windowTo))
                .isEmpty();
        assertThat(attempts.findByRecipient(two.recipientId(), two.tenantId(), windowFrom, windowTo))
                .hasSize(1);
    }

    @Test
    @DisplayName("another tenant's idempotency token resolves to no attempt row")
    void attemptByTokenIsScopedToItsTenant() {
        assertThat(attempts.findByIdempotencyToken(
                two.attemptToken(), one.tenantId(), windowFrom, windowTo))
                .isEmpty();
        assertThat(attempts.findByIdempotencyToken(
                two.attemptToken(), two.tenantId(), windowFrom, windowTo))
                .isPresent();
    }

    @Test
    @DisplayName("the signal history of another tenant's notification is empty, including the refused events")
    void eventHistoryIsScopedToItsTenant() {
        assertThat(events.findByNotification(two.notificationId(), one.tenantId(), windowFrom, windowTo))
                .isEmpty();
        assertThat(events.findUnappliedByNotification(
                two.notificationId(), one.tenantId(), windowFrom, windowTo))
                .as("the unapplied log carries provider codes and error detail; it is not less "
                        + "sensitive than the applied one")
                .isEmpty();

        assertThat(events.findByNotification(two.notificationId(), two.tenantId(), windowFrom, windowTo))
                .hasSize(1);
        assertThat(events.findUnappliedByNotification(
                two.notificationId(), two.tenantId(), windowFrom, windowTo))
                .hasSize(1);
    }

    @Test
    @DisplayName("another tenant's request id returns no row, so its payload and audience ref stay hidden")
    void requestLookupIsScopedToItsTenant() {
        assertThat(requests.findInWindow(two.requestId(), one.tenantId(), windowFrom, windowTo))
                .isEmpty();
        assertThat(requests.findInWindow(two.requestId(), two.tenantId(), windowFrom, windowTo))
                .isPresent();
    }

    @Test
    @DisplayName("expanding a request under the wrong tenant finds no notification to adopt, so one tenant's audience cannot be grafted onto another's campaign")
    void requestFanOutLookupIsScopedToItsTenant() {
        // This finder is the one whose caller writes through what it returns: RequestFanOut adopts
        // the row, hangs the campaign's recipient rows off it and advances its status. Returning
        // another tenant's row here is not a leak, it is a cross-tenant write.
        assertThat(notifications.findByRequest(two.requestId(), one.tenantId(), windowFrom, windowTo))
                .as("nothing to adopt means the expander mints its own row instead of writing into "
                        + "tenant two's")
                .isEmpty();

        assertThat(notifications.findByRequest(two.requestId(), two.tenantId(), windowFrom, windowTo))
                .as("and the owning tenant still finds it, so fan-out keeps adopting the row whose "
                        + "id was returned in the 202")
                .extracting(NotificationEntity::getId)
                .containsExactly(two.notificationId());
    }

    @Test
    @DisplayName("two tenants may use the same idempotency key without seeing each other's claim")
    void idempotencyKeysDoNotCollideAcrossTenants() {
        // Both seeds claim the identical key string on purpose: an idempotency key is chosen by the
        // caller, so 'order-1' is going to exist under every tenant on the platform. Reading one
        // tenant's claim under another would replay a stranger's stored response to them.
        var seenByOne = idempotency.findInWindow(
                one.tenantId(), one.idempotencyKey(), windowFrom, windowTo).orElseThrow();
        var seenByTwo = idempotency.findInWindow(
                two.tenantId(), two.idempotencyKey(), windowFrom, windowTo).orElseThrow();

        assertThat(one.idempotencyKey()).isEqualTo(two.idempotencyKey());
        assertThat(seenByOne.getTenantId()).isEqualTo(one.tenantId());
        assertThat(seenByTwo.getTenantId()).isEqualTo(two.tenantId());
    }

    @Test
    @DisplayName("a status transition aimed at another tenant's notification updates nothing and leaves it PENDING")
    void notificationGuardRefusesACrossTenantTransition() {
        int rowsAffected = notifications.applyStatusTransition(
                two.notificationId(), one.tenantId(), windowFrom, windowTo,
                DeliveryStatus.CANCELLED.name(), (short) DeliveryStatus.CANCELLED.rank(), now);

        assertThat(rowsAffected)
                .as("the guard must refuse it the same way it refuses a stale event: zero rows, no "
                        + "exception")
                .isZero();
        assertThat(statusOfNotification(two))
                .as("CANCELLED is monotonic; had it landed, nothing could ever move this row again")
                .isEqualTo(DeliveryStatus.PENDING);
    }

    @Test
    @DisplayName("a delivery status aimed at another tenant's recipient updates nothing and leaves it QUEUED")
    void recipientGuardRefusesACrossTenantTransition() {
        int rowsAffected = recipients.applyStatusTransition(
                two.recipientId(), one.tenantId(), windowFrom, windowTo,
                DeliveryStatus.BOUNCED.name(), (short) DeliveryStatus.BOUNCED.rank(), now);

        assertThat(rowsAffected).isZero();
        assertThat(statusOfRecipient(two))
                .as("BOUNCED is terminal; a forged one would suppress the address and refuse the "
                        + "real provider callback that follows")
                .isEqualTo(DeliveryStatus.QUEUED);
    }

    @Test
    @DisplayName("the same transition applied by the owning tenant does land, so the guard is refusing the tenant and not the event")
    void theGuardStillAcceptsTheOwningTenant() {
        // The control for the two tests above. Without it, a guard broken so that it refuses
        // everything would look like perfect isolation.
        assertThat(recipients.applyStatusTransition(
                two.recipientId(), two.tenantId(), windowFrom, windowTo,
                DeliveryStatus.BOUNCED.name(), (short) DeliveryStatus.BOUNCED.rank(), now))
                .isEqualTo(1);
        assertThat(statusOfRecipient(two)).isEqualTo(DeliveryStatus.BOUNCED);
    }

    private Fixture seed(String slug) {
        var tenant = new Tenant(slug, slug + " Inc.");
        tenant.setPublicId(UUID.randomUUID());
        long tenantId = tenants.saveAndFlush(tenant).getId();

        var requestId = UUID.randomUUID();
        var notificationId = UUID.randomUUID();
        var recipientId = UUID.randomUUID();
        // Deliberately identical across tenants: a caller-chosen key is not a namespace.
        var idempotencyKey = "order-1";
        var providerMessageId = "SM-shared-vendor-id";
        var attemptToken = "attempt:" + recipientId + ":1";

        requests.saveAndFlush(request(requestId, tenantId));
        notifications.saveAndFlush(notification(notificationId, requestId, tenantId));
        recipients.saveAndFlush(recipient(recipientId, notificationId, tenantId, providerMessageId));
        events.saveAndFlush(event(notificationId, recipientId, tenantId, slug));
        attempts.saveAndFlush(attempt(recipientId, tenantId, attemptToken));
        idempotency.claim(tenantId, idempotencyKey, windowFrom,
                slug.getBytes(StandardCharsets.UTF_8), null,
                now.plus(Duration.ofMinutes(1)), now.plus(Duration.ofHours(24)));

        return new Fixture(tenantId, requestId, notificationId, recipientId,
                idempotencyKey, providerMessageId, attemptToken);
    }

    private NotificationRequest request(UUID id, long tenantId) {
        var row = new NotificationRequest(id, tenantId, TrafficClass.TRANSACTIONAL,
                now.plus(Duration.ofHours(1)));
        row.setCreatedAt(now);
        row.setChannels(new String[] {Channel.SMS.name()});
        row.setScheduleType(ScheduleType.IMMEDIATE);
        row.setRecipientSource(NotificationRequest.RecipientSource.INLINE);
        row.setIdempotencyKey("order-1");
        row.setStatus(NotificationRequest.RequestStatus.ACCEPTED);
        return row;
    }

    private NotificationEntity notification(UUID id, UUID requestId, long tenantId) {
        var row = new NotificationEntity(id, requestId, tenantId, Channel.SMS,
                TrafficClass.TRANSACTIONAL, now.plus(Duration.ofHours(1)));
        row.setCreatedAt(now);
        row.setStatus(DeliveryStatus.PENDING);
        return row;
    }

    private NotificationRecipient recipient(UUID id, UUID notificationId, long tenantId,
                                            String providerMessageId) {
        var row = new NotificationRecipient(id, notificationId, now, tenantId, Channel.SMS,
                new byte[] {1, 2, 3}, new byte[] {4, 5, 6});
        row.setCreatedAt(now);
        row.setStatus(DeliveryStatus.QUEUED);
        row.setProviderMessageId(providerMessageId);
        return row;
    }

    private NotificationEvent event(UUID notificationId, UUID recipientId, long tenantId, String slug) {
        // ne_dedup_uk is UNIQUE (dedup_hash, occurred_at) with no tenant column, so the two
        // tenants' events must differ in the hash or the second insert is rejected by the index.
        // That is the same asymmetry documented on existsByDedupHashInWindow.
        var row = new NotificationEvent(UUID.randomUUID(), now, tenantId, "status.bounced",
                NotificationEvent.EventSource.PROVIDER, ("dedup-" + slug).getBytes(StandardCharsets.UTF_8));
        row.setNotificationId(notificationId);
        row.setRecipientId(recipientId);
        row.setApplied(false);
        return row;
    }

    private DeliveryAttempt attempt(UUID recipientId, long tenantId, String token) {
        var row = new DeliveryAttempt(recipientId, tenantId, (short) 1, (short) 1, token);
        row.setAttemptedAt(now);
        row.setRequestStartedAt(now);
        return row;
    }

    private DeliveryStatus statusOfNotification(Fixture fixture) {
        return notifications
                .findInWindow(fixture.notificationId(), fixture.tenantId(), windowFrom, windowTo)
                .map(NotificationEntity::getStatus)
                .orElseThrow();
    }

    private DeliveryStatus statusOfRecipient(Fixture fixture) {
        return recipients
                .findInWindow(fixture.recipientId(), fixture.tenantId(), windowFrom, windowTo)
                .map(NotificationRecipient::getStatus)
                .orElseThrow();
    }
}
