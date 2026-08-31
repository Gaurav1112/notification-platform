package dev.gaurav.notification.provider.decorator;

import dev.gaurav.notification.domain.enums.Channel;
import dev.gaurav.notification.provider.spi.NotificationProvider;
import dev.gaurav.notification.provider.spi.ProviderCapabilities;
import dev.gaurav.notification.provider.spi.ProviderCode;

import java.util.Objects;

/**
 * Base for every decorator: forwards identity and capability questions untouched.
 *
 * <p>Without this, each decorator hand-writes four pass-through methods, and the first one that
 * forgets — typically {@code capabilities()} — silently reports {@code supportsBatching=false}
 * for a batching provider, quietly turning a 50-destination SES call into 50 calls. Identity must
 * survive wrapping, because the registry, the router and the chaos endpoint all key on
 * {@link #code()}.
 */
public abstract class AbstractProviderDecorator implements NotificationProvider {

    protected final NotificationProvider delegate;

    protected AbstractProviderDecorator(NotificationProvider delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public Channel channel() {
        return delegate.channel();
    }

    @Override
    public ProviderCode code() {
        return delegate.code();
    }

    @Override
    public ProviderCapabilities capabilities() {
        return delegate.capabilities();
    }

    @Override
    public boolean isHealthy() {
        return delegate.isHealthy();
    }

    /** The adapter this decorator wraps. Exposed for assembly and diagnostics, not for bypassing. */
    public final NotificationProvider delegate() {
        return delegate;
    }
}
