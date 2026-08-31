package dev.gaurav.notification.api.filter;

import dev.gaurav.notification.api.error.ApiException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;

/**
 * Rejects an oversized body on the declared {@code Content-Length}, before a byte of it is read.
 *
 * <p><strong>The failure this prevents:</strong> heap pressure from the accept path. A 256 KB
 * ceiling sounds generous until it is multiplied by 200 Tomcat threads and then again by Jackson's
 * intermediate object graph; a handful of concurrent 50 MB posts is a GC pause on a service with a
 * 250 ms p99 budget, and the pause hits every other tenant. Checking the header costs one
 * comparison and happens before the body is buffered.
 *
 * <p>Cheap to defeat by omitting {@code Content-Length} and chunking, which is why the container's
 * own {@code maxSwallowSize} remains the hard backstop. This is the layer that produces a
 * <em>useful</em> error: {@code 413 payload-too-large} naming {@code S3_MANIFEST} as the remedy,
 * rather than a connection reset the caller cannot interpret.
 */
@Component
public class PayloadSizeInterceptor implements org.springframework.web.servlet.HandlerInterceptor {

    /** 256 KB, matching {@code docs/API.md}. Above this the answer is a claim check, not a bigger request. */
    public static final long MAX_BODY_BYTES = 256L * 1024L;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        var declared = request.getContentLengthLong();
        if (declared > MAX_BODY_BYTES) {
            throw ApiException.payloadTooLarge(declared, MAX_BODY_BYTES);
        }
        return true;
    }
}
