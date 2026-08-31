package dev.gaurav.notification.persistence.repository;

import dev.gaurav.notification.persistence.entity.OutboxMessage;
import java.util.Collection;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * The relay's view of the transactional outbox.
 *
 * <p>Two or more relay instances run at once — you cannot have a singleton on the critical path of
 * every send — so the claim has to let them work the same table without stepping on each other
 * and without either of them blocking.
 */
public interface OutboxRepository extends JpaRepository<OutboxMessage, Long> {

    /**
     * Claims the next batch of unpublished messages for this relay instance.
     *
     * <p>{@code FOR UPDATE SKIP LOCKED} is the whole design. Plain {@code FOR UPDATE} would make
     * the second relay <em>wait</em> on the first one's rows, serialising the relays and turning
     * horizontal scaling into a queue; {@code SKIP LOCKED} makes it step over them and take the
     * next unlocked rows instead, so N relays get N disjoint batches with no coordination.
     *
     * <p>{@code ORDER BY id} preserves insert order within a batch, which matters because the
     * outbox is what gives Kafka its per-key ordering. The partial index
     * ({@code WHERE published_at IS NULL}) means the scan touches only unpublished rows, so this
     * query costs the same whether the system has published a thousand messages or a trillion.
     *
     * <p>{@link Propagation#MANDATORY} rather than the default, deliberately. The lock only lives
     * until commit, so a caller without a surrounding transaction would get a lock that is
     * released the instant this method returns — and then two relays would happily publish the
     * same batch. Mandatory turns "forgot the transaction" into an exception at the first call
     * instead of a duplicate-send bug that only appears once a second relay is deployed.
     *
     * @param batchSize how many to take; sized against the publish round trip, not the backlog
     */
    @Transactional(propagation = Propagation.MANDATORY)
    @Query(value = """
            SELECT *
              FROM notif.outbox_message
             WHERE published_at IS NULL
             ORDER BY id
             LIMIT :batchSize
               FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxMessage> claimUnpublished(@Param("batchSize") int batchSize);

    /**
     * Removes published messages.
     *
     * <p>A bulk {@code DELETE} rather than the derived version Spring Data would generate, which
     * loads every entity first: at a thousand messages a batch that is a thousand pointless
     * selects. Deleting rather than stamping {@code published_at} is what keeps the table and its
     * index from growing without bound.
     *
     * <p>Mandatory propagation for the same reason as the claim: the delete has to commit in the
     * same transaction that holds the lock, or the window between them is a republish.
     *
     * @return the number of rows removed
     */
    @Transactional(propagation = Propagation.MANDATORY)
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = "DELETE FROM notif.outbox_message WHERE id IN (:ids)", nativeQuery = true)
    int deleteByIdIn(@Param("ids") Collection<Long> ids);

    /**
     * Counts the unpublished backlog.
     *
     * <p>The alert that matters most on this table: a growing outbox means the relay is down or
     * the broker is unreachable, and every one of those rows is a notification that a caller
     * believes was accepted.
     */
    @Query(value = "SELECT count(*) FROM notif.outbox_message WHERE published_at IS NULL",
            nativeQuery = true)
    long countUnpublished();
}
