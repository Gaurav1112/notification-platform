package dev.gaurav.notification.application.port;

import dev.gaurav.notification.application.result.AcceptResult;

/**
 * Renders an {@link AcceptResult} to the exact JSON the client will receive, so the same bytes can
 * be stored for replay.
 *
 * <p>This exists as a port because the application layer must not know about Jackson, and because
 * the stored idempotent response has to be the response body — not a reconstruction of it. If the
 * replay path rebuilt the JSON from the model, a deploy that renamed a field would make a replay
 * differ from the original response for the same key, which is precisely the guarantee the
 * {@code Idempotency-Key} header sells.
 */
public interface ResponseSerializer {

    /** @return the {@code 202} response body, byte-for-byte as it will be written to the client */
    String serialize(AcceptResult result);
}
