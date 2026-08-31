package dev.gaurav.notification.api.filter;

import dev.gaurav.notification.api.error.ApiException;
import dev.gaurav.notification.api.error.FieldViolation;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerInterceptor;

import java.util.List;

/**
 * Extracts and validates {@code Idempotency-Key} on every handler annotated {@link Idempotent}.
 *
 * <p><strong>The failure this prevents:</strong> a duplicate send caused by somebody else's retry.
 * The accept path sits behind an ALB and in front of clients with their own retry libraries; a
 * request that times out at 250 ms is very often <em>succeeding</em> on the server at 260 ms, and
 * the client retries. Without a key the second request is indistinguishable from a genuine second
 * send, and the user gets two OTPs. Making the header mandatory rather than optional is the whole
 * point: an optional idempotency key is one that the client under load will not send.
 *
 * <p><strong>Rejecting a missing key with 400 rather than generating one server-side</strong> is
 * also deliberate. A server-generated key is different on every retry, which makes it useless and,
 * worse, makes the endpoint <em>look</em> idempotent while providing none of it.
 *
 * <p>Format is bounded because the key becomes half of a primary key in a partitioned table and is
 * echoed into error messages. Constraining it to visible ASCII keeps it out of log-injection
 * territory and out of the index-bloat conversation.
 */
@Component
public class IdempotencyKeyInterceptor implements HandlerInterceptor {

    public static final String HEADER = "Idempotency-Key";

    /** Request attribute the controller reads, so the header is parsed exactly once. */
    public static final String ATTRIBUTE = "notification.idempotencyKey";

    /** Long enough for {@code <service>-<entity>-<uuid>}; short enough to index without regret. */
    static final int MAX_LENGTH = 255;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        if (!(handler instanceof HandlerMethod method) || !method.hasMethodAnnotation(Idempotent.class)) {
            return true;
        }

        var key = request.getHeader(HEADER);
        if (key == null || key.isBlank()) {
            throw ApiException.validationFailed(
                    "The Idempotency-Key header is required on this endpoint. Use a value your retry "
                            + "logic will reproduce — a client-generated UUID, or a key derived from your own entity id.",
                    List.of(FieldViolation.of(HEADER, "MISSING_IDEMPOTENCY_KEY")));
        }
        if (key.length() > MAX_LENGTH || !isVisibleAscii(key)) {
            throw ApiException.validationFailed(
                    "Idempotency-Key must be 1-%d visible ASCII characters.".formatted(MAX_LENGTH),
                    List.of(FieldViolation.of(HEADER, "MALFORMED_IDEMPOTENCY_KEY")));
        }

        request.setAttribute(ATTRIBUTE, key);
        return true;
    }

    /** Printable, non-space ASCII. Excludes CR, LF and tab — the log-injection characters. */
    private static boolean isVisibleAscii(String value) {
        for (var i = 0; i < value.length(); i++) {
            var c = value.charAt(i);
            if (c <= 0x20 || c >= 0x7F) {
                return false;
            }
        }
        return true;
    }
}
