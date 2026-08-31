package dev.gaurav.notification.worker.orchestrator;

import dev.gaurav.notification.domain.enums.Channel;

import java.util.Map;
import java.util.Objects;

/**
 * Turns a template code plus a data map into the subject and body that go on the wire.
 *
 * <p>Rendering happens <strong>once per (channel, locale)</strong> during fan-out, not once per
 * recipient. A 10M-recipient campaign renders the same three strings ten million times otherwise,
 * which is pure CPU with no output difference — the per-recipient part of a notification is the
 * address, not the copy. Per-recipient personalisation, when it arrives, is a merge over the
 * rendered body and belongs behind this same port.
 *
 * <p>A rendering failure is {@link dev.gaurav.notification.domain.enums.FailureType#TEMPLATE_ERROR}
 * — our bug, not the provider's. It must never be retried against a provider and never counted
 * against provider health, which is why it is classified here and not inside a catch block in the
 * channel worker.
 */
public interface TemplateRenderer {

    /**
     * @param subject the email subject, or null for SMS and push where the concept does not exist
     * @param body    the rendered message
     */
    record RenderedMessage(String subject, String body) {
        public RenderedMessage {
            Objects.requireNonNull(body, "body");
        }
    }

    /**
     * @throws TemplateRenderingException when the template is missing or a required variable is
     *         absent — a condition that will recur identically on every retry, so the caller
     *         dead-letters rather than reschedules
     */
    RenderedMessage render(String templateCode, String locale, Channel channel,
                           Map<String, String> data);

    /** Signals {@code TEMPLATE_ERROR}: deterministic, our fault, never retryable. */
    class TemplateRenderingException extends RuntimeException {
        public TemplateRenderingException(String message) {
            super(message);
        }
    }
}
