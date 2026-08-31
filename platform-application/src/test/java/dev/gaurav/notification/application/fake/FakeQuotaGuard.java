package dev.gaurav.notification.application.fake;

import dev.gaurav.notification.application.port.QuotaGuard;

/**
 * A {@link QuotaGuard} that can be told to allow, to deny, or to be unavailable.
 *
 * <p>The third mode is the one that matters: an unavailable limiter throws, and the accept path
 * must admit the request anyway. Distinguishing "unavailable" from "denied" is exactly the
 * behaviour a boolean-returning mock cannot express.
 */
public final class FakeQuotaGuard implements QuotaGuard {

    private enum Mode { ALLOW, DENY, UNAVAILABLE }

    private Mode mode = Mode.ALLOW;
    private int lastPermits;
    private int calls;

    public static FakeQuotaGuard allowing() {
        return new FakeQuotaGuard();
    }

    public FakeQuotaGuard denying() {
        this.mode = Mode.DENY;
        return this;
    }

    /** Simulates Valkey being down: the limiter throws rather than answering "no". */
    public FakeQuotaGuard unavailable() {
        this.mode = Mode.UNAVAILABLE;
        return this;
    }

    @Override
    public boolean tryConsume(String tenantId, int permits) {
        calls++;
        lastPermits = permits;
        return switch (mode) {
            case ALLOW -> true;
            case DENY -> false;
            case UNAVAILABLE -> throw new IllegalStateException("valkey unreachable");
        };
    }

    public int lastPermits() {
        return lastPermits;
    }

    public int calls() {
        return calls;
    }
}
