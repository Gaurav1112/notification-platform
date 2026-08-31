package dev.gaurav.notification.api.rest;

import dev.gaurav.notification.api.dto.ChaosRequest;
import dev.gaurav.notification.api.dto.ChaosResponse;
import dev.gaurav.notification.api.error.ApiException;
import dev.gaurav.notification.api.error.ProblemType;
import dev.gaurav.notification.provider.mock.ChaosState;
import dev.gaurav.notification.provider.registry.ProviderRegistry;
import dev.gaurav.notification.provider.spi.ProviderCode;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.Objects;

/**
 * Breaks a named mock provider on demand, so failover is demonstrable in thirty seconds instead of
 * describable in ten minutes.
 *
 * <pre>
 * POST /admin/v1/mock-providers/mock-sms-primary/chaos {"mode":"HARD_DOWN","durationSeconds":120}
 *   → breaker OPEN at t+2s → failover to mock-sms-secondary → half-open probe → CLOSED
 * </pre>
 *
 * <p><strong>Two independent guards keep this out of production, and one would not be enough.</strong>
 * The bean only exists when {@code notification.providers.mock-chaos.enabled} is true, and that
 * property lives beside {@code notification.providers.mock.enabled} — so a deployment running real
 * providers has no chaos endpoint at all, not a protected one. {@code matchIfMissing = false} is the
 * important half: an environment whose config forgot to mention chaos gets none, rather than
 * inheriting a permissive default. A route that exists and is merely authorised is one
 * misconfigured role binding away from being an outage button, and it would still show up in the
 * OpenAPI document as something an operator could try.
 *
 * <p>Mounted under {@code /admin/v1} rather than {@code /v1} because the admin surface is expected
 * to sit behind a separate ingress with SSO and MFA; keeping the prefix distinct is what makes that
 * routing rule expressible.
 *
 * <p>An unknown provider code answers 404 through the same exception the notification endpoints use
 * — there is no reason for this endpoint to disclose the registry's contents more freely than any
 * other.
 */
@RestController
@RequestMapping("${notification.providers.mock-chaos.admin-path:/admin/v1/mock-providers}")
@ConditionalOnProperty(prefix = "notification.providers.mock-chaos", name = "enabled",
        havingValue = "true", matchIfMissing = false)
@Tag(name = "Chaos (local/dev only)", description = "Inject bounded faults into mock providers")
public class ChaosController {

    private static final Logger log = LoggerFactory.getLogger(ChaosController.class);

    private final ChaosState chaos;
    private final ProviderRegistry registry;

    public ChaosController(ChaosState chaos, ProviderRegistry registry) {
        this.chaos = Objects.requireNonNull(chaos, "chaos");
        this.registry = Objects.requireNonNull(registry, "registry");
        log.warn("chaos endpoint is ACTIVE; mock providers can be broken on demand. "
                + "This must never be true in production.");
    }

    /**
     * Inject a bounded fault.
     *
     * <p>The response echoes the resolved deadline rather than the requested duration, because
     * {@link ChaosState} clamps anything over an hour. An operator who asked for two weeks should
     * see that they did not get it.
     */
    @PostMapping("/{code}/chaos")
    @Operation(summary = "Inject a fault into a mock provider for a bounded window")
    public ChaosResponse inject(@PathVariable String code, @Valid @RequestBody ChaosRequest request) {
        var provider = requireKnown(code);
        var window = chaos.setMode(provider, request.mode(), request.duration().orElse(null));
        log.warn("chaos injected: provider={} mode={} until={}", code, window.mode(), window.until());
        return ChaosResponse.of(code, window);
    }

    /** Clear a fault early, for when the demo finishes ahead of the two-minute window. */
    @DeleteMapping("/{code}/chaos")
    @Operation(summary = "Clear an injected fault immediately")
    public ChaosResponse clear(@PathVariable String code) {
        // setMode(NORMAL, ...) removes the window outright rather than scheduling a "be healthy"
        // fault, so this is a clear and not a second injection.
        return ChaosResponse.of(code, chaos.setMode(requireKnown(code), ChaosState.Mode.NORMAL, null));
    }

    /** Every fault currently in force. The answer to "why is the demo still broken". */
    @GetMapping("/chaos")
    @Operation(summary = "List active faults")
    public Map<String, ChaosState.Window> active() {
        return chaos.active();
    }

    private ProviderCode requireKnown(String code) {
        var providerCode = ProviderCode.of(code);
        if (registry.byCode(providerCode).isEmpty()) {
            // Injecting into a code nobody routes to would appear to work and change nothing --
            // the most confusing possible outcome for a live demo.
            throw new ApiException(ProblemType.NOTIFICATION_NOT_FOUND,
                    "No provider is registered under code '%s'.".formatted(code));
        }
        return providerCode;
    }
}
