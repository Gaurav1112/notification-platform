package dev.gaurav.notification.scheduler.due;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.domain.enums.FailureType;
import dev.gaurav.notification.domain.enums.TrafficClass;
import dev.gaurav.notification.messaging.event.DeadLetterEvent;
import dev.gaurav.notification.messaging.producer.NotificationEventPublisher;
import dev.gaurav.notification.scheduler.config.SchedulerProperties;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The reaper is the only thing that gets work back from a pod that died holding it, and the only
 * thing that notices a row which kills every pod that touches it. Both paths are asserted here,
 * along with the ordering between them.
 */
class LeaseReaperTest {

    private static final Instant NOW = Instant.parse("2026-09-01T09:00:00Z");
    /** Matches {@code notification.scheduler.claimer.max-claims-per-row}. */
    private static final int MAX_CLAIMS = 5;

    private final ScheduledWorkStore store = mock(ScheduledWorkStore.class);
    private final NotificationEventPublisher publisher = mock(NotificationEventPublisher.class);
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

    private LeaseReaper reaper;

    @BeforeEach
    void setUp() {
        var properties = new SchedulerProperties(null,
                new SchedulerProperties.Claimer(null, null, null, MAX_CLAIMS),
                null, null, null, null);
        reaper = new LeaseReaper(store, publisher, properties,
                Clock.fixed(NOW, ZoneOffset.UTC), meters);
        when(publisher.publishDeadLetter(any())).thenReturn(CompletableFuture.completedFuture(null));
    }

    @Test
    @DisplayName("a pod OOM-killed mid-dispatch has its work handed back, not lost forever")
    void expiredLeaseIsReturnedToTheReadySet() {
        var abandoned = work(UUID.randomUUID(), 0);
        when(store.findExpiredLeases(any(), anyInt())).thenReturn(List.of(abandoned));
        when(store.reclaim(anyList(), any())).thenReturn(1);

        reaper.reap();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<UUID>> ids = ArgumentCaptor.forClass(Collection.class);
        verify(store).reclaim(ids.capture(), any());
        assertThat(ids.getValue())
                .as("nothing else will ever move a CLAIMED row whose owner is gone")
                .containsExactly(abandoned.id());
        verify(publisher, never()).publishDeadLetter(any());
        assertThat(meters.counter("scheduler.lease.expired").count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("a row that has killed a pod six times goes to the DLQ instead of round again")
    void poisonRowIsDeadLetteredRatherThanReclaimed() {
        var poison = work(UUID.randomUUID(), MAX_CLAIMS + 1);
        when(store.findExpiredLeases(any(), anyInt())).thenReturn(List.of(poison));

        reaper.reap();

        verify(store, never()).reclaim(anyList(), any());
        var event = ArgumentCaptor.forClass(DeadLetterEvent.class);
        verify(publisher).publishDeadLetter(event.capture());
        assertThat(event.getValue().recipientId()).isEqualTo(poison.recipientId());
        assertThat(event.getValue().attemptCount()).isEqualTo(MAX_CLAIMS + 1);
        assertThat(event.getValue().failureType()).isEqualTo(FailureType.PERMANENT_UNKNOWN);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<UUID>> abandoned = ArgumentCaptor.forClass(Collection.class);
        verify(store).abandon(abandoned.capture());
        assertThat(abandoned.getValue()).containsExactly(poison.id());
        assertThat(meters.counter("scheduler.claim.count.exceeded").count())
                .as("without this counter the crash loop is invisible: the pod restarts cleanly, "
                        + "the group looks healthy, and the shard silently does nothing")
                .isEqualTo(1.0);
    }

    @Test
    @DisplayName("a row exactly at the limit still gets one more chance")
    void theThresholdIsExclusive() {
        var borderline = work(UUID.randomUUID(), MAX_CLAIMS);
        when(store.findExpiredLeases(any(), anyInt())).thenReturn(List.of(borderline));
        when(store.reclaim(anyList(), any())).thenReturn(1);

        reaper.reap();

        verify(store).reclaim(anyList(), any());
        verify(publisher, never()).publishDeadLetter(any());
    }

    @Test
    @DisplayName("the DLQ record is written before the row is marked FAILED, never after")
    void deadLetterIsAcknowledgedBeforeTheRowDisappears() {
        var poison = work(UUID.randomUUID(), MAX_CLAIMS + 3);
        when(store.findExpiredLeases(any(), anyInt())).thenReturn(List.of(poison));

        reaper.reap();

        var order = inOrder(publisher, store);
        order.verify(publisher).publishDeadLetter(any());
        order.verify(store).abandon(anyList());
    }

    @Test
    @DisplayName("a DLQ that refuses the record must not let the row vanish from every queue")
    void abandonIsSkippedWhenTheDeadLetterPublishFails() {
        var poison = work(UUID.randomUUID(), MAX_CLAIMS + 1);
        when(store.findExpiredLeases(any(), anyInt())).thenReturn(List.of(poison));
        when(publisher.publishDeadLetter(any()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")));

        reaper.reap();

        verify(store, never()).abandon(anyList());
        assertThat(meters.counter("scheduler.claim.count.exceeded").count())
                .as("the row is still stuck; claiming otherwise would be a lie on the dashboard")
                .isZero();
    }

    @Test
    @DisplayName("one poison row in a batch does not stop the healthy leases being reclaimed")
    void healthyAndPoisonRowsAreSeparatedWithinOneBatch() {
        var healthy = work(UUID.randomUUID(), 1);
        var poison = work(UUID.randomUUID(), MAX_CLAIMS + 1);
        when(store.findExpiredLeases(any(), anyInt())).thenReturn(List.of(healthy, poison));
        when(store.reclaim(anyList(), any())).thenReturn(1);

        reaper.reap();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<UUID>> reclaimed = ArgumentCaptor.forClass(Collection.class);
        verify(store).reclaim(reclaimed.capture(), any());
        assertThat(reclaimed.getValue()).containsExactly(healthy.id());
        verify(publisher).publishDeadLetter(any());
    }

    private static ScheduledWork work(UUID id, int claimCount) {
        return new ScheduledWork(id, NOW.minusSeconds(120), 7, 42L,
                UUID.randomUUID(), NOW.minusSeconds(600), UUID.randomUUID(),
                Channel.SMS, TrafficClass.TRANSACTIONAL,
                claimCount, "scheduler-1/dead", NOW.minusSeconds(60), "{}");
    }
}
