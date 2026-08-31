package dev.gaurav.notification.persistence.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.time.Instant;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * How one provider is wired up, either globally ({@code tenantId == null}) or for one tenant.
 *
 * <p>{@code credentialsRef} is a <em>pointer</em> — {@code arn:aws:secretsmanager:…},
 * {@code ssm:…} or {@code mock:…} — and the database enforces that with a CHECK constraint. Code
 * review can miss a pasted API key; a check constraint cannot. Any attempt to store the secret
 * itself fails the INSERT.
 *
 * <p>{@code timeoutMs} is bounded at 60 s by the schema because it has to stay well below Kafka's
 * {@code max.poll.interval.ms}. A provider that hangs longer than the poll interval gets the
 * consumer evicted from its group, and the resulting rebalance stalls the whole partition — a
 * slow vendor turning into an outage for every tenant on that topic.
 *
 * <p>{@link Version} is appropriate here for the same reason as on {@link Tenant}: these rows are
 * edited by people, rarely, and a lost update is worth reporting rather than silently merging.
 */
@Entity
@Table(name = "provider_configuration", schema = "notif")
public class ProviderConfiguration {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false, updatable = false)
    private Long id;

    @Column(name = "provider_id", nullable = false)
    private Short providerId;

    /** Null means "the platform default for this provider"; a value scopes it to one tenant. */
    @Column(name = "tenant_id")
    private Long tenantId;

    @Column(name = "routing_priority", nullable = false)
    private short routingPriority = 100;

    @Column(name = "weight", nullable = false)
    private short weight = 100;

    @Column(name = "rate_limit_per_sec", nullable = false)
    private int rateLimitPerSec = 1000;

    @Column(name = "daily_cap")
    private Long dailyCap;

    /** A secret-manager reference, never the secret. Enforced by {@code ..._secret_ck}. */
    @Column(name = "credentials_ref", nullable = false)
    private String credentialsRef;

    @Column(name = "endpoint_url")
    private String endpointUrl;

    // TODO(phase-4): promote to a typed settings record once the provider adapters land and the
    // set of keys is known. Kept as raw JSON text so an unrecognised key from an older or newer
    // deploy round-trips instead of being dropped on read-modify-write.
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "settings", nullable = false)
    private String settings = "{}";

    @Column(name = "retry_policy_id", nullable = false)
    private Short retryPolicyId;

    @Column(name = "unit_cost_micros", nullable = false)
    private long unitCostMicros;

    @Column(name = "timeout_ms", nullable = false)
    private int timeoutMs = 5000;

    @Column(name = "is_active", nullable = false)
    private boolean active = true;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt = Instant.now();

    @Version
    // Boxed, not primitive: Spring Data uses a null version to recognise a brand-new
    // entity. With a primitive it falls back to the assigned id, decides the row might
    // already exist, and turns every insert into a pointless SELECT first.
    @Column(name = "row_version", nullable = false)
    private Long rowVersion;

    protected ProviderConfiguration() {
        // for JPA
    }

    public ProviderConfiguration(Short providerId, Short retryPolicyId, String credentialsRef) {
        this.providerId = providerId;
        this.retryPolicyId = retryPolicyId;
        this.credentialsRef = credentialsRef;
    }

    public Long getId() {
        return id;
    }

    public Short getProviderId() {
        return providerId;
    }

    public void setProviderId(Short providerId) {
        this.providerId = providerId;
    }

    public Long getTenantId() {
        return tenantId;
    }

    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    public short getRoutingPriority() {
        return routingPriority;
    }

    public void setRoutingPriority(short routingPriority) {
        this.routingPriority = routingPriority;
    }

    public short getWeight() {
        return weight;
    }

    public void setWeight(short weight) {
        this.weight = weight;
    }

    public int getRateLimitPerSec() {
        return rateLimitPerSec;
    }

    public void setRateLimitPerSec(int rateLimitPerSec) {
        this.rateLimitPerSec = rateLimitPerSec;
    }

    public Long getDailyCap() {
        return dailyCap;
    }

    public void setDailyCap(Long dailyCap) {
        this.dailyCap = dailyCap;
    }

    public String getCredentialsRef() {
        return credentialsRef;
    }

    public void setCredentialsRef(String credentialsRef) {
        this.credentialsRef = credentialsRef;
    }

    public String getEndpointUrl() {
        return endpointUrl;
    }

    public void setEndpointUrl(String endpointUrl) {
        this.endpointUrl = endpointUrl;
    }

    public String getSettings() {
        return settings;
    }

    public void setSettings(String settings) {
        this.settings = settings;
    }

    public Short getRetryPolicyId() {
        return retryPolicyId;
    }

    public void setRetryPolicyId(Short retryPolicyId) {
        this.retryPolicyId = retryPolicyId;
    }

    public long getUnitCostMicros() {
        return unitCostMicros;
    }

    public void setUnitCostMicros(long unitCostMicros) {
        this.unitCostMicros = unitCostMicros;
    }

    public int getTimeoutMs() {
        return timeoutMs;
    }

    public void setTimeoutMs(int timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    public Long getRowVersion() {
        return rowVersion;
    }
}
