package dev.gaurav.notification.adapter.persistence;

import dev.gaurav.notification.application.port.IdempotencyStore;
import dev.gaurav.notification.persistence.entity.IdempotencyRecord;
import dev.gaurav.notification.persistence.repository.IdempotencyRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Bridges {@link IdempotencyStore} onto {@code notif.idempotency_record} through
 * {@link IdempotencyRepository}.
 *
 * <p>The seam exists so the accept use case can state "claim this key" without knowing that the
 * claim is an {@code INSERT … ON CONFLICT DO NOTHING} against an hourly-partitioned table whose
 * primary key leads with the partition column. That last detail is the whole reason this class is
 * more than a one-line delegation, and it is worth stating plainly:
 *
 * <p><strong>The primary key is {@code (created_at, tenant_id, idempotency_key)}.</strong> A
 * conflict therefore only fires when {@code created_at} matches exactly, and a wall-clock
 * {@code created_at} never repeats. So the insert alone cannot serialise two requests carrying the
 * same key. This adapter closes that by writing every claim at the <em>hour boundary</em>
 * ({@link PartitionWindows#hourSlot}), which is also the partition boundary: two pods racing on the
 * same key within the same hour produce byte-identical primary keys, exactly one insert wins, and
 * the loser reads the winner's row. A prior claim from an earlier hour is found by the lookup that
 * runs first, so a key is claimed at most once inside its 24-hour lifetime.
 *
 * <p>TODO(phase-3): two requests carrying the same key that straddle an hour boundary within the
 * same few milliseconds can both claim. Closing it needs the slot to be derived from the key rather
 * than from the clock — {@code created_at = hour_of(hash(key))} — which changes the partition
 * pruning of the expiry sweep and is a schema decision, not an adapter one.
 *
 * <p>An {@code IN_PROGRESS} record whose {@code locked_until} has elapsed is treated as claimable
 * rather than as a conflict. That is the fencing semantics the column exists for: the previous
 * holder died before it stored a response, so no response was ever sent for this key and redoing
 * the work is the correct recovery. Reporting {@code IN_PROGRESS} forever instead would wedge the
 * key until its 24-hour expiry.
 */
@Component
public class JpaIdempotencyStore implements IdempotencyStore {

    /**
     * How long a claim fences out a concurrent request.
     *
     * <p>Comfortably longer than the 250 ms accept budget so a legitimately slow accept is not
     * taken over mid-transaction, and short enough that a crashed pod does not block a client's
     * retry for a noticeable time.
     */
    private static final Duration LOCK_TTL = Duration.ofSeconds(60);

    private static final Logger log = LoggerFactory.getLogger(JpaIdempotencyStore.class);

    private final IdempotencyRepository records;
    private final TenantDirectory tenants;
    private final Clock clock;

    public JpaIdempotencyStore(IdempotencyRepository records, TenantDirectory tenants, Clock clock) {
        this.records = Objects.requireNonNull(records, "records");
        this.tenants = Objects.requireNonNull(tenants, "tenants");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public ClaimResult claim(String tenantId, String key, byte[] fingerprint) {
        long tenant = tenants.requireInternalId(tenantId);
        var now = clock.instant();
        var slot = PartitionWindows.hourSlot(now);

        var existing = lookup(tenant, key, slot);
        if (existing.isPresent()) {
            return interpret(existing.get(), fingerprint, now);
        }

        int inserted = records.claim(tenant, key, slot, fingerprint, null,
                now.plus(LOCK_TTL), slot.plus(PartitionWindows.IDEMPOTENCY_RETENTION));
        if (inserted == 1) {
            return ClaimResult.claimed();
        }

        // Zero rows: another request inserted the same (slot, tenant, key) between our lookup and
        // our insert. The unique index decided the race; read the winner's row rather than guess.
        return lookup(tenant, key, slot)
                .map(record -> interpret(record, fingerprint, now))
                .orElseGet(() -> {
                    // The row vanished between the failed insert and the re-read, which only
                    // happens if expiry dropped its partition underneath us. Reporting in-progress
                    // asks the client to retry, which is the one safe answer.
                    log.warn("idempotency claim for key '{}' conflicted but no record is visible", key);
                    return ClaimResult.inProgress();
                });
    }

    @Override
    public void complete(String tenantId, String key, int status, String body) {
        long tenant = tenants.requireInternalId(tenantId);
        var slot = PartitionWindows.hourSlot(clock.instant());
        int updated = records.complete(tenant, key,
                slot.minus(PartitionWindows.IDEMPOTENCY_RETENTION), slot.plus(Duration.ofHours(1)),
                (short) status, body,
                // TODO(phase-3): IdempotencyStore.complete carries no request id, so the
                // idempotency row cannot point back at the notification_request it guarded. The
                // column stays null; the join is available through the stored response body.
                null);
        if (updated == 0) {
            // The record was fenced by a sweep, or someone else completed it first. The accept
            // transaction has already committed, so this is a lost replay opportunity and not a
            // failed send — the caller must not see a 500 for it.
            log.warn("idempotency record for key '{}' was no longer IN_PROGRESS; "
                    + "a retry of this key will re-run the accept rather than replay", key);
        }
    }

    @Override
    public Optional<StoredResponse> find(String tenantId, String key) {
        return tenants.internalIdOf(tenantId)
                .flatMap(tenant -> lookup(tenant, key, PartitionWindows.hourSlot(clock.instant())))
                .flatMap(JpaIdempotencyStore::storedResponseOf);
    }

    /**
     * Reads across the 24-hour retention window rather than a single partition.
     *
     * <p>That is ~25 hourly partitions per probe, which the planner prunes to and which the primary
     * key serves. It is the cost of a key whose slot is not derivable from the key itself; see the
     * class-level TODO.
     */
    private Optional<IdempotencyRecord> lookup(long tenant, String key, Instant slot) {
        return records.findInWindow(tenant, key,
                slot.minus(PartitionWindows.IDEMPOTENCY_RETENTION), slot.plus(Duration.ofHours(1)));
    }

    private ClaimResult interpret(IdempotencyRecord record, byte[] fingerprint, Instant now) {
        // Constant-time, because this compares a hash of caller-supplied bytes against a stored
        // one and a short-circuiting Arrays.equals leaks the matching prefix length.
        if (!MessageDigest.isEqual(record.getRequestFingerprint(), fingerprint)) {
            return ClaimResult.conflict();
        }
        var stored = storedResponseOf(record);
        if (stored.isPresent()) {
            return ClaimResult.replay(stored.get());
        }
        var lockedUntil = record.getLockedUntil();
        if (record.getState() == IdempotencyRecord.State.IN_PROGRESS
                && lockedUntil != null && lockedUntil.isAfter(now)) {
            return ClaimResult.inProgress();
        }
        // Expired lock, or a FAILED record: no response was ever stored for this key, so nothing
        // can be replayed and the work has to be redone.
        return ClaimResult.claimed();
    }

    private static Optional<StoredResponse> storedResponseOf(IdempotencyRecord record) {
        if (record.getState() != IdempotencyRecord.State.COMPLETED || record.getResponseBody() == null) {
            return Optional.empty();
        }
        var status = record.getResponseStatus();
        return Optional.of(new StoredResponse(status == null ? 202 : status, record.getResponseBody()));
    }
}
