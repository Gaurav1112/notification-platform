package dev.gaurav.notification.application.fake;

import dev.gaurav.notification.application.port.ResponseSerializer;
import dev.gaurav.notification.application.result.AcceptResult;

/**
 * Deterministic stand-in for the Jackson-backed serializer.
 *
 * <p>The shape does not matter to these tests; what matters is that the bytes handed to
 * {@code complete()} are the same bytes a replay hands back, so the tests can assert on identity
 * rather than on JSON structure.
 */
public final class FakeResponseSerializer implements ResponseSerializer {

    @Override
    public String serialize(AcceptResult result) {
        var ids = result.notifications().stream()
                .map(n -> "\"" + n.id() + "\"")
                .toList();
        return "{\"notificationRequestId\":\"" + result.requestId() + "\""
                + ",\"status\":\"" + AcceptResult.STATUS_ACCEPTED + "\""
                + ",\"recipientCount\":" + result.recipientCount()
                + ",\"notifications\":" + ids + "}";
    }
}
