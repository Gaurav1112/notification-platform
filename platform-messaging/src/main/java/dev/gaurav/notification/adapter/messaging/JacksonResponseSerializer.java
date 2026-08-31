package dev.gaurav.notification.adapter.messaging;

import dev.gaurav.notification.application.port.ResponseSerializer;
import dev.gaurav.notification.application.result.AcceptResult;

import tools.jackson.databind.json.JsonMapper;

/**
 * The fallback {@link ResponseSerializer}: renders an {@link AcceptResult} with the platform's
 * JSON mapper.
 *
 * <p>The seam exists because the application layer must not import Jackson, and because the stored
 * idempotent response has to <em>be</em> the response body rather than a reconstruction of it. If
 * the replay path rebuilt the JSON from a reloaded model, a deploy that renamed a field would make
 * a replay differ from the original response for the same key — precisely the guarantee the
 * {@code Idempotency-Key} header sells.
 *
 * <p><strong>This implementation is a fallback, not the real one.</strong> It exists so that
 * {@code app-worker} and {@code app-scheduler} — which component-scan
 * {@code dev.gaurav.notification} and therefore instantiate {@code AcceptNotificationUseCase} even
 * though neither ever calls it — have a complete bean graph. The deployable that actually owns the
 * HTTP contract, {@code app-api}, supplies its own implementation that serialises the exact
 * {@code AcceptResponse} the client receives, and that one wins by
 * {@code @ConditionalOnMissingBean}. Storing bytes that are not the response body would make a
 * replay differ from the original in field names, which is the one thing this port exists to
 * prevent — so this class must never be the one in front of a client.
 *
 * <p>Registered from {@link MessagingAdapterConfiguration} rather than annotated
 * {@code @Component}: a conditional evaluated during component scanning depends on scan order, so
 * whether the real implementation won would depend on its package name.
 *
 * <p>Boot 4 ships Jackson 3, so the type is {@code tools.jackson.databind.json.JsonMapper} and a
 * serialisation failure is unchecked. Failing to serialise our own record is a programming error
 * and must surface rather than be logged.
 */
public class JacksonResponseSerializer implements ResponseSerializer {

    private final JsonMapper mapper;

    public JacksonResponseSerializer(JsonMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public String serialize(AcceptResult result) {
        // A replay carries the original bytes verbatim; re-serialising the wrapper around them
        // would nest one response inside another.
        return result.replayed()
                .map(AcceptResult.Replay::body)
                .orElseGet(() -> mapper.writeValueAsString(result));
    }
}
