package dev.gaurav.notification.application.port;

import java.util.Map;
import java.util.Objects;

/**
 * Turns a template code plus variables into the subject and body that will actually be sent.
 *
 * <p><strong>Deliberately not on the accept path.</strong> Rendering needs the template store, the
 * locale catalogue and the variables schema — three dependencies whose latency and availability
 * would become the latency and availability of {@code POST /notifications}. It runs in the
 * orchestrator instead, where a template outage delays delivery rather than rejecting a request
 * that was otherwise perfectly valid.
 *
 * <p>Locale fallback belongs to the implementation, not the caller: {@code fr-CA → fr → en}. Doing
 * it at call sites means one code path forgets, and a French-Canadian user gets an English SMS from
 * one endpoint and a French one from another.
 */
public interface TemplateRenderer {

    /**
     * @param code   the template code, e.g. {@code order-shipped}
     * @param locale BCP-47 tag; the implementation falls back rather than failing on an exact miss
     * @param vars   validated against the template version's {@code variablesSchema}
     * @throws RuntimeException when the template is missing or a required variable is absent — a
     *                          notification body reading "Hello ${firstName}" is worse than no
     *                          notification, because it is unrecallable
     */
    RenderedContent render(String code, String locale, Map<String, Object> vars);

    /**
     * @param locale the locale actually used, which may differ from the one requested after
     *               fallback — recorded so support can answer "why was this in English?"
     */
    record RenderedContent(String subject, String body, String locale) {

        public RenderedContent {
            Objects.requireNonNull(body, "body");
            Objects.requireNonNull(locale, "locale");
        }
    }
}
