package dev.gaurav.notification.messaging.producer;

/**
 * A produce that was <em>not</em> acknowledged by the broker before its caller had to decide
 * whether to commit an offset.
 *
 * <p>Unchecked on purpose, and thrown rather than logged. Every consumer in this platform
 * acknowledges its offset at the end of the listener method; the only way to stop an unconfirmed
 * publish from being followed by {@code ack.acknowledge()} is for the listener to leave by an
 * exception. A logged-and-swallowed failure here is exactly the silent loss this type exists to
 * end: the offset advances, the dispatch event never reached Kafka, and the recipient stays
 * {@code QUEUED} forever with no retry, no dead letter and no alert.
 *
 * <p>Throwing is safe because every consumer that can raise this is idempotent — the record is
 * redelivered, the idempotent receiver and the monotonic status guard absorb the replay, and the
 * publish is attempted again. Redelivery is the cheap failure; a committed offset over a lost
 * event is the expensive one.
 *
 * @see PublishBatch
 */
public class PublishNotConfirmedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public PublishNotConfirmedException(String message) {
        super(message);
    }

    public PublishNotConfirmedException(String message, Throwable cause) {
        super(message, cause);
    }
}
