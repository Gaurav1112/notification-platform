package dev.gaurav.notification.api.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Who may reach what.
 *
 * <p>Three groups of endpoints are open, each for a different and defensible reason:
 *
 * <ul>
 *   <li><strong>{@code /actuator/health}, {@code /actuator/info}, {@code /actuator/prometheus}</strong>
 *       — the kubelet and the scraper have no bearer token and cannot get one. A 401 here secures
 *       nothing and makes liveness probes fail, taking the service down by itself. The <em>rest</em>
 *       of the actuator surface stays authenticated on purpose: {@code /actuator/env} and
 *       {@code /actuator/heapdump} are a credential dump, and blanket-permitting {@code /actuator/**}
 *       is how they end up public.</li>
 *   <li><strong>{@code /v3/api-docs}, {@code /swagger-ui}</strong> — the contract is committed to
 *       the repository, so authenticating it protects nothing that is not already published.</li>
 *   <li><strong>{@code /v1/webhooks/**}</strong> — <em>authenticated, but not by this filter
 *       chain.</em> A provider posting a delivery receipt has an HMAC over the raw payload and no
 *       session, no cookie and no token; there is nothing here for a session-based filter to check.
 *       The authentication moves into the handler, where the signature can be verified against the
 *       exact bytes received. Permitting the path here is what makes that possible — it is not an
 *       unauthenticated endpoint.</li>
 * </ul>
 *
 * <p><strong>CSRF is disabled, and that is correct here rather than lazy.</strong> CSRF defends
 * browser sessions authenticated by an ambient credential the browser attaches automatically. This
 * API is stateless and authenticated by an {@code Authorization} header that no browser attaches on
 * its own, so there is no ambient credential to forge with. Leaving it on would break every
 * non-browser client and protect nothing. The corollary — enforced below with
 * {@link SessionCreationPolicy#STATELESS} — is that a session must never be created, because the
 * moment a session cookie exists that reasoning stops holding.
 */
@Configuration
@EnableWebSecurity
@EnableConfigurationProperties({ApiSecurityProperties.class, WebhookProperties.class})
public class SecurityConfig {

    private static final Logger log = LoggerFactory.getLogger(SecurityConfig.class);

    private static final String[] OPEN_PATHS = {
            "/actuator/health", "/actuator/health/**", "/actuator/info", "/actuator/prometheus",
            "/v3/api-docs", "/v3/api-docs/**", "/swagger-ui.html", "/swagger-ui/**",
            "/v1/webhooks/**"
    };

    /**
     * The chain used everywhere that is not a developer laptop.
     *
     * <p>Authentication is still {@code httpBasic} because
     * {@code spring-boot-starter-oauth2-resource-server} is not on this module's classpath. The
     * <em>authorisation topology</em> below is the real one and does not change when the JWT decoder
     * arrives: only the {@code httpBasic} line is replaced by
     * {@code oauth2ResourceServer(o -> o.jwt(...))}. Keeping the topology honest now is what stops
     * that swap from quietly opening something.
     */
    @Bean
    @ConditionalOnProperty(prefix = "notification.api.security", name = "permit-all",
            havingValue = "false", matchIfMissing = true)
    public SecurityFilterChain apiSecurityFilterChain(HttpSecurity http) throws Exception {
        return http
                .csrf(AbstractHttpConfigurer::disable)
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth
                        // Preflight carries no credentials by definition; a 401 here turns every
                        // browser call into a CORS error that looks like a bug in the client.
                        .requestMatchers(HttpMethod.OPTIONS, "/**").permitAll()
                        .requestMatchers(OPEN_PATHS).permitAll()
                        .anyRequest().authenticated())
                .httpBasic(Customizer.withDefaults())
                .build();
    }

    /**
     * Local and CI only: everything is open and every caller is the fixed development tenant.
     *
     * <p><strong>This exists so {@code ./mvnw spring-boot:run} produces a working demo with no
     * token, and it is gated by an opt-in property rather than by a profile.</strong> A profile is a
     * string on a command line, trivially copied into a production manifest. A property that must be
     * explicitly set to {@code true}, defaults to {@code false}, and prints this banner on every
     * startup is much harder to enable by accident and impossible to enable silently.
     */
    @Bean
    @ConditionalOnProperty(prefix = "notification.api.security", name = "permit-all", havingValue = "true")
    public SecurityFilterChain permitAllFilterChain(HttpSecurity http) throws Exception {
        log.warn("=================================================================");
        log.warn(" SECURITY IS DISABLED: notification.api.security.permit-all=true");
        log.warn(" Every request is attributed to the local development tenant.");
        log.warn(" This must never be set outside a laptop or a CI run.");
        log.warn("=================================================================");
        return http
                .csrf(AbstractHttpConfigurer::disable)
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth.anyRequest().permitAll())
                .build();
    }
}
