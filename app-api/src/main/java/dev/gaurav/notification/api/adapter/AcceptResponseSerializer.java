package dev.gaurav.notification.api.adapter;

import dev.gaurav.notification.api.dto.AcceptResponse;
import dev.gaurav.notification.application.port.ResponseSerializer;
import dev.gaurav.notification.application.result.AcceptResult;
import dev.gaurav.notification.messaging.config.KafkaProducerConfig;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.util.Objects;

/**
 * The authoritative {@link ResponseSerializer}: it stores the bytes of the {@link AcceptResponse}
 * the client is about to receive, not a serialisation of the internal {@link AcceptResult}.
 *
 * <p><strong>That distinction is the whole point of the port.</strong> The idempotency record holds
 * the response to replay, and a replay must be the same response — same field names, same ids, same
 * shape. Storing the application's read model instead would mean a retry that took the replay path
 * received a body with different field names from the one the first caller got, for the same key.
 * The endpoint would look idempotent and would not be.
 *
 * <p>Two concrete failures the round trip through {@code AcceptResult} produced before this class
 * existed, both worth naming because neither is visible until a replay actually happens:
 *
 * <ul>
 *   <li>{@code AcceptResult} exposes a derived {@code isReplay()} alongside its {@code replay}
 *       component, so Jackson wrote {@code "replay": false} where a {@code Replay} object belongs
 *       and refused to read its own output back — a {@code 500} on the second identical request,
 *       which is the exact request idempotency exists to make safe.</li>
 *   <li>Its field names ({@code requestId}) differ from the wire contract's
 *       ({@code notificationRequestId}), so even a successful round trip would have answered a
 *       retry with a body no client could parse.</li>
 * </ul>
 *
 * <p>This bean overrides the fallback registered by {@code MessagingAdapterConfiguration}, which
 * exists only so the worker and scheduler contexts — which instantiate the accept use case without
 * ever calling it — have a complete bean graph.
 */
@Component
public class AcceptResponseSerializer implements ResponseSerializer {

    private final JsonMapper mapper;

    public AcceptResponseSerializer(@Qualifier(KafkaProducerConfig.EVENT_JSON_MAPPER) JsonMapper mapper) {
        this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    @Override
    public String serialize(AcceptResult result) {
        // A replay already holds the original bytes; re-serialising would nest one response in
        // another and change the very bytes this port exists to preserve.
        return result.replayed()
                .map(AcceptResult.Replay::body)
                .orElseGet(() -> mapper.writeValueAsString(AcceptResponses.from(result)));
    }
}
