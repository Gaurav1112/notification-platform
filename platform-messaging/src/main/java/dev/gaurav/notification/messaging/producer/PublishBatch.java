package dev.gaurav.notification.messaging.producer;

import dev.gaurav.notification.messaging.event.NotificationEvent;

import org.springframework.kafka.support.SendResult;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The produces one listener invocation started, and the one question that must be answered before
 * its offset is committed: <strong>did every one of them reach the broker?</strong>
 *
 * <p>{@code kafkaTemplate.send()} is asynchronous, and a broker outage does not throw from it — it
 * completes the returned future exceptionally, seconds or minutes later. A listener that discards
 * that future and calls {@code ack.acknowledge()} has committed the offset for a record whose
 * effect never left the JVM. During a 30-second broker blip in a campaign that is every dispatch
 * event in flight: recipients sit in {@code QUEUED} forever, with no retry, no dead letter and no
 * alert, because from Kafka's point of view the work was done. This class exists so that the ack
 * cannot happen before the confirmation.
 *
 * <p><strong>One deadline for the whole batch, applied at the point of waiting.</strong> Two
 * consequences, both deliberate:
 *
 * <ul>
 *   <li>The budget passed to {@link #awaitAll(Duration)} bounds the <em>wait</em>, not the work
 *       that produced it. A channel worker adds its first future before an 8-second provider call
 *       and waits afterwards; a deadline started at construction would already have expired.</li>
 *   <li>The deadline is computed once and each {@code get} is given only what is left of it. A
 *       per-future timeout of 10 s across a 500-event fan-out is 83 minutes of one consumer thread
 *       in the worst case — far past {@code max.poll.interval.ms}, so the cure would be a rebalance
 *       storm. Because the sends are all in flight concurrently, the batch normally completes in
 *       about the time of the slowest single send, not the sum.</li>
 * </ul>
 *
 * <p>Nanosecond arithmetic on {@link System#nanoTime()} rather than wall-clock instants: the
 * deadline must survive an NTP step, and a clock adjusted backwards mid-wait would otherwise
 * extend the wait past the poll interval.
 *
 * <p>Not thread-safe, and not meant to be. One instance belongs to one listener invocation on one
 * consumer thread.
 */
public final class PublishBatch {

    private final List<CompletableFuture<SendResult<String, NotificationEvent>>> inFlight;

    /** For a caller that publishes a handful of events and does not know the count up front. */
    public PublishBatch() {
        this(4);
    }

    /** @param expectedSize sizes the list once, for a fan-out that already knows its recipient count */
    public PublishBatch(int expectedSize) {
        this.inFlight = new ArrayList<>(Math.max(1, expectedSize));
    }

    /**
     * Registers one in-flight produce.
     *
     * <p>Returns {@code this} so a single-publish call site reads as one statement and cannot
     * accidentally add the future and then await a different, empty batch.
     */
    public PublishBatch add(CompletableFuture<SendResult<String, NotificationEvent>> future) {
        inFlight.add(Objects.requireNonNull(future, "future"));
        return this;
    }

    /** How many produces are still waiting to be confirmed. Zero after a successful await. */
    public int size() {
        return inFlight.size();
    }

    /**
     * Blocks until every registered produce has been acknowledged by the broker, or throws.
     *
     * <p>Call this immediately before {@code ack.acknowledge()} and nowhere else. On return, every
     * send has an ack from a leader that satisfied {@code acks=all}, and committing the offset is
     * safe. On {@link PublishNotConfirmedException} the caller must let the exception escape the
     * listener: the offset then stays where it is and the record is redelivered.
     *
     * <p>A timeout is treated as a failure, not as a maybe. The producer's own
     * {@code delivery.timeout.ms} is 120 s, so a future that has not completed within a budget
     * measured in seconds is still retrying internally and may yet succeed — which would make this
     * a duplicate, not a loss. That trade is the right way round: every consumer here is
     * idempotent, so a duplicate is absorbed, and a loss is not recoverable at all.
     *
     * <p>Successfully awaited futures are discarded, so a batch that is awaited twice does not wait
     * twice and a 10M-recipient fan-out does not hold its futures beyond the confirmation.
     *
     * @param budget the total time this call may spend waiting. Must be comfortably below
     *               {@code max.poll.interval.ms} divided by {@code max.poll.records} — see the
     *               arithmetic at each call site
     * @throws PublishNotConfirmedException if any send failed, timed out, or the thread was
     *                                      interrupted while waiting
     */
    public void awaitAll(Duration budget) {
        if (inFlight.isEmpty()) {
            return;
        }
        long deadlineNanos = System.nanoTime() + budget.toNanos();
        for (int i = 0; i < inFlight.size(); i++) {
            awaitOne(inFlight.get(i), i, deadlineNanos, budget);
        }
        inFlight.clear();
    }

    private void awaitOne(CompletableFuture<SendResult<String, NotificationEvent>> future,
                          int index, long deadlineNanos, Duration budget) {
        try {
            if (future.isDone()) {
                // Already acknowledged, or already failed. Either way there is nothing to wait for,
                // and spending the remaining budget on a decision that is made would let a long
                // batch of completed sends expire the deadline for the one send still in flight.
                future.get();
                return;
            }
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0L) {
                throw timedOut(index, budget, null);
            }
            future.get(remainingNanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            // Shutdown. Restore the flag so the container stops rather than looping, and still
            // refuse to confirm: an interrupted wait proves nothing about the send.
            Thread.currentThread().interrupt();
            throw new PublishNotConfirmedException(
                    "interrupted while waiting for Kafka to acknowledge " + progress(index), e);
        } catch (TimeoutException e) {
            throw timedOut(index, budget, e);
        } catch (ExecutionException e) {
            throw new PublishNotConfirmedException(
                    "Kafka rejected a publish after " + progress(index), e.getCause());
        }
    }

    private PublishNotConfirmedException timedOut(int index, Duration budget, Throwable cause) {
        String message = "Kafka did not acknowledge within " + budget.toMillis() + "ms after "
                + progress(index) + "; refusing to commit the offset";
        return cause == null
                ? new PublishNotConfirmedException(message)
                : new PublishNotConfirmedException(message, cause);
    }

    private String progress(int index) {
        return index + " of " + inFlight.size() + " sends";
    }
}
