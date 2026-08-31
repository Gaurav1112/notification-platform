package dev.gaurav.notification.api.config;

import dev.gaurav.notification.api.filter.ApiCallerArgumentResolver;
import dev.gaurav.notification.api.filter.IdempotencyKeyInterceptor;
import dev.gaurav.notification.api.filter.PayloadSizeInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.List;
import java.util.Objects;

/**
 * Wires the request-scoped concerns that must run for every handler.
 *
 * <p>Order matters and is explicit. {@link PayloadSizeInterceptor} runs first so a 50 MB body is
 * rejected before anything else touches the request; {@link IdempotencyKeyInterceptor} runs second
 * so a handler that creates work cannot execute without a key. Relying on bean-definition order
 * here would make that ordering an accident of classpath scanning.
 *
 * <p>Both are registered against {@code /**} rather than a path list. The idempotency interceptor
 * decides by method annotation, so a broad path costs one {@code instanceof} on requests it does
 * not apply to — and a path list is the thing that silently stops covering the next endpoint.
 */
@Configuration
public class WebMvcConfig implements WebMvcConfigurer {

    private final PayloadSizeInterceptor payloadSize;
    private final IdempotencyKeyInterceptor idempotencyKey;
    private final ApiCallerArgumentResolver apiCaller;

    public WebMvcConfig(PayloadSizeInterceptor payloadSize,
                        IdempotencyKeyInterceptor idempotencyKey,
                        ApiCallerArgumentResolver apiCaller) {
        this.payloadSize = Objects.requireNonNull(payloadSize, "payloadSize");
        this.idempotencyKey = Objects.requireNonNull(idempotencyKey, "idempotencyKey");
        this.apiCaller = Objects.requireNonNull(apiCaller, "apiCaller");
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(payloadSize).order(0);
        registry.addInterceptor(idempotencyKey).order(10);
    }

    @Override
    public void addArgumentResolvers(List<HandlerMethodArgumentResolver> resolvers) {
        resolvers.add(apiCaller);
    }
}
