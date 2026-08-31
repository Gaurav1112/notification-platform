package dev.gaurav.notification.provider.mock.admin;

import dev.gaurav.notification.provider.mock.ChaosState;

import java.time.Instant;

/**
 * What was injected and, more importantly, when it stops.
 *
 * <p>{@code until} is echoed because the caller asked for a duration and the server may have
 * clamped it. Returning the resolved deadline rather than the requested duration means the operator
 * running {@code make demo} can see at a glance that their two-week outage became one hour.
 */
public record ChaosResponse(String provider, ChaosState.Mode mode, Instant until) {

    public static ChaosResponse of(String provider, ChaosState.Window window) {
        return new ChaosResponse(provider, window.mode(), window.until());
    }
}
