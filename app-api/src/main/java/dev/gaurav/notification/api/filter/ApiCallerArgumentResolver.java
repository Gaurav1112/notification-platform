package dev.gaurav.notification.api.filter;

import dev.gaurav.notification.api.config.ApiSecurityProperties;
import dev.gaurav.notification.api.error.ApiException;
import dev.gaurav.notification.api.port.ApiCaller;
import org.springframework.core.MethodParameter;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Turns the authenticated principal into an {@link ApiCaller} handler parameter.
 *
 * <p><strong>Why a resolver rather than each controller reading the SecurityContext.</strong> The
 * tenant id is the scoping key for every query in the system, and reading it from a static
 * {@code ThreadLocal} inside a controller makes it invisible in the method signature — which is
 * exactly what lets someone later add an endpoint that forgets to scope. A declared
 * {@code ApiCaller} parameter puts the tenant in the signature, makes it trivially mockable, and
 * makes an unscoped handler look wrong at a glance.
 *
 * <p>Scopes arrive as Spring authorities. The {@code SCOPE_} prefix is stripped here so the rest of
 * the codebase talks about {@code notifications:send} — the string in the OAuth2 token and in
 * {@code docs/API.md} — rather than a framework-shaped variant of it.
 */
@Component
public class ApiCallerArgumentResolver implements HandlerMethodArgumentResolver {

    private static final String SCOPE_PREFIX = "SCOPE_";

    /**
     * Authority carrying the tenant, used until a real resource server is wired in.
     *
     * <p>With {@code spring-boot-starter-oauth2-resource-server} on the classpath this becomes
     * {@code jwt.getClaimAsString(properties.tenantClaim())} and this constant disappears. The
     * shape of {@link ApiCaller} does not change, which is the point of resolving here.
     */
    private static final String TENANT_AUTHORITY_PREFIX = "TENANT_";

    private final ApiSecurityProperties properties;

    public ApiCallerArgumentResolver(ApiSecurityProperties properties) {
        this.properties = Objects.requireNonNull(properties, "properties");
    }

    @Override
    public boolean supportsParameter(MethodParameter parameter) {
        return ApiCaller.class.equals(parameter.getParameterType());
    }

    @Override
    public ApiCaller resolveArgument(MethodParameter parameter, ModelAndViewContainer mav,
                                     NativeWebRequest request, WebDataBinderFactory binderFactory) {
        var authentication = SecurityContextHolder.getContext().getAuthentication();
        if (isAuthenticated(authentication)) {
            var authorities = authentication.getAuthorities();
            return new ApiCaller(tenantOf(authorities), authentication.getName(), scopesOf(authorities));
        }
        if (properties.permitAll()) {
            // Only reachable when the permit-all chain is active, which SecurityConfig refuses to
            // activate outside a development profile.
            return new ApiCaller(properties.localTenantId(), "local-development", properties.localScopes());
        }
        throw ApiException.unauthenticated("A bearer token is required.");
    }

    private static boolean isAuthenticated(Authentication authentication) {
        return authentication != null
                && authentication.isAuthenticated()
                && !(authentication instanceof AnonymousAuthenticationToken);
    }

    private UUID tenantOf(Collection<? extends GrantedAuthority> authorities) {
        return authorities.stream()
                .map(GrantedAuthority::getAuthority)
                .filter(a -> a.startsWith(TENANT_AUTHORITY_PREFIX))
                .map(a -> a.substring(TENANT_AUTHORITY_PREFIX.length()))
                .map(ApiCallerArgumentResolver::parseUuid)
                .flatMap(Optional::stream)
                .findFirst()
                // A token with no tenant is a misconfigured client, not an unlucky one. Failing
                // closed here is the difference between "your token is wrong" and "your request
                // silently ran against the wrong tenant".
                .orElseThrow(() -> ApiException.unauthenticated("Token carries no tenant identity."));
    }

    private static Optional<UUID> parseUuid(String candidate) {
        try {
            return Optional.of(UUID.fromString(candidate));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    private static Set<String> scopesOf(Collection<? extends GrantedAuthority> authorities) {
        return authorities.stream()
                .map(GrantedAuthority::getAuthority)
                .filter(a -> a.startsWith(SCOPE_PREFIX))
                .map(a -> a.substring(SCOPE_PREFIX.length()))
                .collect(Collectors.toUnmodifiableSet());
    }
}
