package dev.gaurav.notification.api.filter;

import io.micrometer.tracing.Tracer;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

/**
 * Establishes the two correlation identifiers every log line and every error body carries, and
 * guarantees they are gone again before the thread is reused.
 *
 * <p>Two identifiers, because they answer different questions:
 * <ul>
 *   <li>{@code requestId} — the caller's. Echoed back so a client can quote it in a support ticket
 *       and we can find their exact request without knowing anything about our own tracing stack.</li>
 *   <li>{@code traceId} — ours. Joins this request to the Kafka produce, the worker dispatch and
 *       the provider call that happen minutes later on other machines.</li>
 * </ul>
 *
 * <p><strong>The inbound header is sanitised, not trusted.</strong> It is written into MDC, which
 * ends up in log output, and an unbounded attacker-controlled string there is log injection: a
 * newline forges a log line, and a megabyte header multiplied by request rate is a disk-fill.
 * Anything longer than {@link #MAX_LENGTH} or containing characters outside a conservative set is
 * replaced rather than rejected — a bad correlation header is not worth failing a send over.
 *
 * <p><strong>The {@code finally} block is load-bearing.</strong> MDC is a {@code ThreadLocal} and
 * Tomcat pools threads, so a missed clear does not lose the id — it attaches it to the <em>next</em>
 * request, and the resulting logs point an investigation at the wrong tenant entirely.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class RequestIdFilter extends OncePerRequestFilter {

    public static final String REQUEST_ID_HEADER = "X-Request-Id";
    public static final String MDC_REQUEST_ID = "requestId";
    public static final String MDC_TRACE_ID = "traceId";

    /** Long enough for a UUID or a ULID with a prefix; short enough not to be a log-volume weapon. */
    static final int MAX_LENGTH = 128;

    private final ObjectProvider<Tracer> tracer;

    /**
     * {@code Tracer} is taken as an {@link ObjectProvider} so this filter still works in a slice
     * test, or in any deployment where tracing is switched off. Correlation must not be a casualty
     * of disabling observability.
     */
    public RequestIdFilter(ObjectProvider<Tracer> tracer) {
        this.tracer = tracer;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain chain) throws ServletException, IOException {
        var requestId = sanitise(request.getHeader(REQUEST_ID_HEADER));
        var traceId = currentTraceId().orElse(requestId);

        MDC.put(MDC_REQUEST_ID, requestId);
        MDC.put(MDC_TRACE_ID, traceId);
        request.setAttribute(MDC_TRACE_ID, traceId);
        // Set before the chain runs: a response committed by a downstream filter or an async
        // dispatch would otherwise be sent without the header the caller was promised.
        response.setHeader(REQUEST_ID_HEADER, requestId);

        try {
            chain.doFilter(request, response);
        } finally {
            MDC.remove(MDC_REQUEST_ID);
            MDC.remove(MDC_TRACE_ID);
        }
    }

    /**
     * The trace id the error body reports, resolved from whatever is available.
     *
     * <p>Falls back to the request attribute, then MDC, then a fresh value, so a problem response
     * assembled outside the filter chain — an authentication entry point, say — still carries
     * something a support engineer can search on.
     */
    public static String traceIdOf(HttpServletRequest request) {
        if (request != null && request.getAttribute(MDC_TRACE_ID) instanceof String attribute) {
            return attribute;
        }
        var fromMdc = MDC.get(MDC_TRACE_ID);
        return fromMdc != null ? fromMdc : newId();
    }

    private Optional<String> currentTraceId() {
        var active = tracer.getIfAvailable();
        if (active == null) {
            return Optional.empty();
        }
        var span = active.currentSpan();
        return span == null ? Optional.empty() : Optional.of(span.context().traceId());
    }

    /** Conservative allowlist: anything that could forge a log line or a header is discarded. */
    private static String sanitise(String candidate) {
        if (candidate == null || candidate.isBlank() || candidate.length() > MAX_LENGTH) {
            return newId();
        }
        for (var i = 0; i < candidate.length(); i++) {
            var c = candidate.charAt(i);
            var allowed = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
                    || c == '-' || c == '_' || c == '.' || c == ':';
            if (!allowed) {
                return newId();
            }
        }
        return candidate;
    }

    /** 32 lowercase hex characters — the W3C trace-id shape, so it looks native in any backend. */
    private static String newId() {
        return UUID.randomUUID().toString().replace("-", "");
    }
}
