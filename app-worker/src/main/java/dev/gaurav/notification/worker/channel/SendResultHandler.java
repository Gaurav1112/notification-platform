package dev.gaurav.notification.worker.channel;

import dev.gaurav.notification.provider.spi.SendResult;

/**
 * Forces every caller to answer for all three {@link SendResult} cases, including
 * {@link SendResult.Indeterminate}.
 *
 * <p>The design calls for a pattern switch over the sealed interface, which is what makes the
 * compiler reject a caller that forgot {@code Indeterminate}. Pattern matching for {@code switch}
 * is JEP 441, <strong>final in Java 21</strong>; this module compiles with
 * {@code maven.compiler.release=17}, where it is a preview feature and {@code --enable-preview}
 * produces class files pinned to one exact JVM version — not something to ship in a deployable.
 * So the exhaustiveness is reproduced structurally instead:
 *
 * <ul>
 *   <li>The {@code instanceof} chain exists exactly once, in {@link #dispatch}, rather than being
 *       copy-pasted into three channel workers where one of the copies will eventually be
 *       missing a branch.</li>
 *   <li>Every handler must implement three methods. There is no default for
 *       {@code onIndeterminate}, deliberately: a default would let "the provider may or may not
 *       have delivered" quietly inherit the failure path, which is how a user receives three
 *       one-time passcodes.</li>
 *   <li>A fourth case added to {@code SendResult} adds a method here and breaks every
 *       implementation at compile time — the same signal the switch would give.</li>
 * </ul>
 *
 * <p>TODO(java-21): delete this interface and inline a pattern switch at the call site.
 *
 * @param <T> what the caller wants back — an outcome record, a status event, a decision
 */
public interface SendResultHandler<T> {

    /** The provider acknowledged it. */
    T onAccepted(SendResult.Accepted accepted);

    /** The provider refused it, and told us why. */
    T onRejected(SendResult.Rejected rejected);

    /**
     * The request left the building and no answer came back.
     *
     * <p>Not a failure. Treating it as one and retrying is the single most expensive mistake
     * available on this path: for SMS there is no reconciliation API on the dominant vendor, so a
     * blind retry is an unrecoverable duplicate charged to the tenant and delivered to a real
     * person.
     */
    T onIndeterminate(SendResult.Indeterminate indeterminate);

    /**
     * The one place the sealed hierarchy is destructured.
     *
     * @throws IllegalStateException if {@code SendResult} gains a case and this method is not
     *         updated — loud at the first call rather than silently taking a wrong branch
     */
    static <T> T dispatch(SendResult result, SendResultHandler<T> handler) {
        if (result instanceof SendResult.Accepted accepted) {
            return handler.onAccepted(accepted);
        }
        if (result instanceof SendResult.Rejected rejected) {
            return handler.onRejected(rejected);
        }
        if (result instanceof SendResult.Indeterminate indeterminate) {
            return handler.onIndeterminate(indeterminate);
        }
        throw new IllegalStateException(
                "unhandled SendResult case: " + result.getClass().getName());
    }
}
