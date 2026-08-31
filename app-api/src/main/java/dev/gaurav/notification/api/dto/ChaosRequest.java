package dev.gaurav.notification.api.dto;

import dev.gaurav.notification.provider.mock.ChaosState;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.time.Duration;
import java.util.Optional;

/**
 * Body of the chaos endpoint: break a named mock provider for a bounded window.
 *
 * <p>{@code durationSeconds} is optional but never unbounded — {@link ChaosState} clamps it to one
 * hour and defaults it to two minutes. <strong>A fault with no deadline is how a shared demo
 * environment stays broken until somebody remembers what they did on Tuesday.</strong> The upper
 * bound here is a second line of defence so that a fat-fingered {@code 1200000} is rejected with a
 * 400 explaining the cap rather than silently truncated.
 */
public record ChaosRequest(

        @NotNull
        ChaosState.Mode mode,

        @Positive
        @Max(value = 3600, message = "no injected fault may last longer than an hour")
        Integer durationSeconds
) {

    /** Empty lets {@link ChaosState} apply its own default rather than duplicating the number here. */
    public Optional<Duration> duration() {
        return Optional.ofNullable(durationSeconds).map(Duration::ofSeconds);
    }
}
