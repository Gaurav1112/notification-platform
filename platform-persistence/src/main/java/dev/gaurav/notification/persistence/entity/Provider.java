package dev.gaurav.notification.persistence.entity;

import dev.gaurav.notification.domain.enums.Channel;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

/**
 * A downstream vendor we can hand a message to — the persistent half of
 * {@code dev.gaurav.notification.provider.spi.NotificationProvider}.
 *
 * <p>The capability flags are stored rather than asked of the adapter at runtime because routing
 * has to reason about them <em>before</em> a provider is selected, and because they belong to the
 * contract we signed, not to the code. {@code supportsIdempotencyKey} is the one that changes
 * behaviour most: it is false for essentially every real vendor, which is precisely why the
 * {@code UNKNOWN} reconciliation path exists. If it were true we could simply re-send after a
 * timeout.
 *
 * <p>The id is a {@code smallint} because there will never be three hundred of these and the
 * value is copied onto ten billion {@code delivery_attempt} rows.
 */
@Entity
@Table(name = "provider", schema = "notif")
public class Provider {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false, updatable = false)
    private Short id;

    @Column(name = "code", nullable = false, length = 32)
    private String code;

    @Column(name = "display_name", nullable = false)
    private String displayName;

    @Enumerated(EnumType.STRING)
    @Column(name = "channel", nullable = false, length = 16)
    private Channel channel;

    @Column(name = "vendor", nullable = false, length = 32)
    private String vendor;

    @Column(name = "supports_batching", nullable = false)
    private boolean supportsBatching;

    @Column(name = "max_batch_size", nullable = false)
    private int maxBatchSize = 1;

    /**
     * False for every vendor we would plausibly integrate. Drives the {@code UNKNOWN}
     * reconciliation path: without a provider-honoured idempotency key, a timeout cannot be
     * retried blindly without risking a duplicate OTP.
     */
    @Column(name = "supports_idempotency_key", nullable = false)
    private boolean supportsIdempotencyKey;

    @Column(name = "supports_webhook", nullable = false)
    private boolean supportsWebhook = true;

    @Column(name = "is_active", nullable = false)
    private boolean active = true;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    protected Provider() {
        // for JPA
    }

    public Provider(String code, String displayName, Channel channel, String vendor) {
        this.code = code;
        this.displayName = displayName;
        this.channel = channel;
        this.vendor = vendor;
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

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public String getVendor() {
        return vendor;
    }

    public void setVendor(String vendor) {
        this.vendor = vendor;
    }

    public boolean isSupportsBatching() {
        return supportsBatching;
    }

    public void setSupportsBatching(boolean supportsBatching) {
        this.supportsBatching = supportsBatching;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    public boolean isSupportsIdempotencyKey() {
        return supportsIdempotencyKey;
    }

    public void setSupportsIdempotencyKey(boolean supportsIdempotencyKey) {
        this.supportsIdempotencyKey = supportsIdempotencyKey;
    }

    public boolean isSupportsWebhook() {
        return supportsWebhook;
    }

    public void setSupportsWebhook(boolean supportsWebhook) {
        this.supportsWebhook = supportsWebhook;
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
}
