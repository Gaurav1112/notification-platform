package dev.gaurav.notification.persistence.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;

/**
 * A named backoff schedule, referenced by {@link ProviderConfiguration}.
 *
 * <p>Named {@code RetryPolicyEntity} rather than {@code RetryPolicy} so it cannot be confused
 * with the domain-side policy object that actually computes a delay; this class is only the row.
 *
 * <p>{@code totalDeadlineMs} is separate from {@code maxAttempts} on purpose. Attempts alone are
 * not a bound on wall-clock time: five attempts with an hour of backoff between them is a
 * twenty-hour retry chain, and delivering a one-time passcode twenty hours late is worse than not
 * delivering it. Whichever limit trips first wins.
 */
@Entity
@Table(name = "retry_policy", schema = "notif")
public class RetryPolicyEntity {

    /**
     * How much randomness to add to each backoff interval.
     *
     * <p>{@code FULL} is the default for a reason that shows up only under load: with
     * {@code NONE} or {@code EQUAL}, a mass failure schedules every retry at the same instant and
     * the resulting synchronised wave re-kills the provider that just recovered.
     */
    public enum JitterStrategy {
        NONE,
        EQUAL,
        FULL,
        DECORRELATED
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false, updatable = false)
    private Short id;

    @Column(name = "code", nullable = false, length = 32)
    private String code;

    @Column(name = "max_attempts", nullable = false)
    private short maxAttempts;

    @Column(name = "initial_backoff_ms", nullable = false)
    private int initialBackoffMs;

    @Column(name = "max_backoff_ms", nullable = false)
    private int maxBackoffMs;

    @Column(name = "backoff_multiplier", nullable = false, precision = 4, scale = 2)
    private BigDecimal backoffMultiplier = new BigDecimal("2.00");

    @Enumerated(EnumType.STRING)
    @Column(name = "jitter_strategy", nullable = false, length = 16)
    private JitterStrategy jitterStrategy = JitterStrategy.FULL;

    @Column(name = "total_deadline_ms", nullable = false)
    private int totalDeadlineMs = 86_400_000;

    protected RetryPolicyEntity() {
        // for JPA
    }

    public Short getId() {
        return id;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public short getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(short maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public int getInitialBackoffMs() {
        return initialBackoffMs;
    }

    public void setInitialBackoffMs(int initialBackoffMs) {
        this.initialBackoffMs = initialBackoffMs;
    }

    public int getMaxBackoffMs() {
        return maxBackoffMs;
    }

    public void setMaxBackoffMs(int maxBackoffMs) {
        this.maxBackoffMs = maxBackoffMs;
    }

    public BigDecimal getBackoffMultiplier() {
        return backoffMultiplier;
    }

    public void setBackoffMultiplier(BigDecimal backoffMultiplier) {
        this.backoffMultiplier = backoffMultiplier;
    }

    public JitterStrategy getJitterStrategy() {
        return jitterStrategy;
    }

    public void setJitterStrategy(JitterStrategy jitterStrategy) {
        this.jitterStrategy = jitterStrategy;
    }

    public int getTotalDeadlineMs() {
        return totalDeadlineMs;
    }

    public void setTotalDeadlineMs(int totalDeadlineMs) {
        this.totalDeadlineMs = totalDeadlineMs;
    }
}
