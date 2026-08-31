package dev.gaurav.notification.persistence.repository;

import dev.gaurav.notification.persistence.entity.DeadLetterMessage;
import java.time.Instant;
import java.util.List;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

/**
 * The triage queue. Not partitioned, so no window is required — see
 * {@link DeadLetterMessage} for why that is deliberate.
 */
public interface DeadLetterMessageRepository extends JpaRepository<DeadLetterMessage, Long> {

    List<DeadLetterMessage> findByTriageState(DeadLetterMessage.TriageState triageState, Limit limit);

    List<DeadLetterMessage> findBySourceTopicAndTriageState(String sourceTopic,
                                                            DeadLetterMessage.TriageState triageState,
                                                            Limit limit);

    /**
     * Records a poisoned message, collapsing repeats into one row.
     *
     * <p>{@code ON CONFLICT … DO UPDATE SET occurrence_count = occurrence_count + 1} is what makes
     * this table survivable. One bad deserialiser against a full topic produces four million
     * failures; without the upsert that is four million rows and an unreadable triage queue, and
     * the insert traffic itself becomes a second incident. With it, roughly thirty rows carrying
     * high counts.
     *
     * <p>Note the conflict target must name all three columns of {@code dlm_dedup_uk} for index
     * inference to match.
     *
     * @return 1 in both cases — inserted or counted; the caller does not need to distinguish
     */
    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            INSERT INTO notif.dead_letter_message
                        (first_seen_at, last_seen_at, tenant_id, notification_id, source_topic,
                         source_partition, source_offset, message_key, payload, error_class,
                         error_message, stack_digest)
                 VALUES (:seenAt, :seenAt, :tenantId, NULL, :sourceTopic,
                         :sourcePartition, :sourceOffset, :messageKey, CAST(:payload AS jsonb),
                         :errorClass, :errorMessage, :stackDigest)
            ON CONFLICT (source_topic, message_key, stack_digest)
              DO UPDATE SET occurrence_count = dead_letter_message.occurrence_count + 1,
                            last_seen_at     = EXCLUDED.last_seen_at
            """, nativeQuery = true)
    int record(@Param("seenAt") Instant seenAt,
               @Param("tenantId") Long tenantId,
               @Param("sourceTopic") String sourceTopic,
               @Param("sourcePartition") Integer sourcePartition,
               @Param("sourceOffset") Long sourceOffset,
               @Param("messageKey") String messageKey,
               @Param("payload") String payload,
               @Param("errorClass") String errorClass,
               @Param("errorMessage") String errorMessage,
               @Param("stackDigest") byte[] stackDigest);

    /** Backlog size by triage state; the number that says whether anyone is working the queue. */
    @Query("SELECT d.triageState, count(d) FROM DeadLetterMessage d GROUP BY d.triageState")
    List<Object[]> countByTriageState();
}
