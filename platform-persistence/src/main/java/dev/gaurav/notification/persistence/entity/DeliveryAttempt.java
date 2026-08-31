package dev.gaurav.notification.persistence.entity;

import dev.gaurav.notification.domain.enums.AttemptState;
import dev.gaurav.notification.domain.enums.FailureType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/**
 * One network call to one provider. Append-only: these rows are inserted and never updated in
 * place by the retry engine, which is what makes them usable as an audit trail and as the input
 * to the provider-health rollup.
 *
 * <p>The row is written <strong>before</strong> the network call, with
 * {@code state = PENDING} and {@code requestStartedAt} set. That ordering is the whole point: a
 * worker that dies mid-call leaves a visible {@code PENDING} row instead of an invisible gap, and
 * reconciliation can find it. Write the row afterwards and a crash between send and persist is
 * indistinguishable from never having sent — which is how a user gets a second OTP.
 *
 * <p>{@code idempotencyToken} is unique per attempt (enforced by {@code da_token_uk}), so a
 * redelivered Kafka message cannot create a second attempt row for the same logical send.
 *
 * <p><strong>Partitioned by {@code attempted_at} (daily); {@code @Id} is {@code id} alone.</strong>
 * Every query must carry an {@code attempted_at} bound. Note the identity generator: it costs a
 * {@code RETURNING} round trip per row and disables JDBC batching, which is acceptable here only
 * because attempts are written one at a time on the send path anyway. Do not copy the pattern to
 * a table that is bulk-inserted.
 */
@Entity
@Table(name = "delivery_attempt", schema = "notif")
public class DeliveryAttempt {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false, updatable = false)
    private Long id;

    /** Partition key. */
    @Column(name = "attempted_at", nullable = false, updatable = false)
    private Instant attemptedAt = Instant.now();

    @Column(name = "recipient_id", nullable = false)
    private UUID recipientId;

    @Column(name = "tenant_id", nullable = false)
    private Long tenantId;

    @Column(name = "attempt_no", nullable = false)
    private short attemptNo;

    @Column(name = "provider_id", nullable = false)
    private Short providerId;

    /** Sent to the provider; unique so a redelivered message cannot duplicate the attempt. */
    @Column(name = "idempotency_token", nullable = false, length = 128)
    private String idempotencyToken;

    @Enumerated(EnumType.STRING)
    @Column(name = "state", nullable = false, length = 16)
    private AttemptState state = AttemptState.PENDING;

    /** Written before the network call, so a crash leaves a PENDING row rather than a gap. */
    @Column(name = "request_started_at", nullable = false)
    private Instant requestStartedAt = Instant.now();

    @Column(name = "response_at")
    private Instant responseAt;

    @Column(name = "latency_ms")
    private Integer latencyMs;

    @Column(name = "http_status")
    private Short httpStatus;

    @Column(name = "provider_message_id", length = 128)
    private String providerMessageId;

    @Enumerated(EnumType.STRING)
    @Column(name = "failure_type", length = 32)
    private FailureType failureType;

    @Column(name = "error_code", length = 64)
    private String errorCode;

    @Column(name = "error_detail")
    private String errorDetail;

    @Column(name = "cost_micros")
    private Long costMicros;

    protected DeliveryAttempt() {
        // for JPA
    }

    public DeliveryAttempt(UUID recipientId, Long tenantId, short attemptNo, Short providerId,
                           String idempotencyToken) {
        this.recipientId = recipientId;
        this.tenantId = tenantId;
        this.attemptNo = attemptNo;
        this.providerId = providerId;
        this.idempotencyToken = idempotencyToken;
    }

    public Long getId() {
        return id;
    }

    public Instant getAttemptedAt() {
        return attemptedAt;
    }

    public void setAttemptedAt(Instant attemptedAt) {
        this.attemptedAt = attemptedAt;
    }

    public UUID getRecipientId() {
        return recipientId;
    }

    public void setRecipientId(UUID recipientId) {
        this.recipientId = recipientId;
    }

    public Long getTenantId() {
        return tenantId;
    }

    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    public short getAttemptNo() {
        return attemptNo;
    }

    public void setAttemptNo(short attemptNo) {
        this.attemptNo = attemptNo;
    }

    public Short getProviderId() {
        return providerId;
    }

    public void setProviderId(Short providerId) {
        this.providerId = providerId;
    }

    public String getIdempotencyToken() {
        return idempotencyToken;
    }

    public void setIdempotencyToken(String idempotencyToken) {
        this.idempotencyToken = idempotencyToken;
    }

    public AttemptState getState() {
        return state;
    }

    public void setState(AttemptState state) {
        this.state = state;
    }

    public Instant getRequestStartedAt() {
        return requestStartedAt;
    }

    public void setRequestStartedAt(Instant requestStartedAt) {
        this.requestStartedAt = requestStartedAt;
    }

    public Instant getResponseAt() {
        return responseAt;
    }

    public void setResponseAt(Instant responseAt) {
        this.responseAt = responseAt;
    }

    public Integer getLatencyMs() {
        return latencyMs;
    }

    public void setLatencyMs(Integer latencyMs) {
        this.latencyMs = latencyMs;
    }

    public Short getHttpStatus() {
        return httpStatus;
    }

    public void setHttpStatus(Short httpStatus) {
        this.httpStatus = httpStatus;
    }

    public String getProviderMessageId() {
        return providerMessageId;
    }

    public void setProviderMessageId(String providerMessageId) {
        this.providerMessageId = providerMessageId;
    }

    public FailureType getFailureType() {
        return failureType;
    }

    public void setFailureType(FailureType failureType) {
        this.failureType = failureType;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorDetail() {
        return errorDetail;
    }

    public void setErrorDetail(String errorDetail) {
        this.errorDetail = errorDetail;
    }

    public Long getCostMicros() {
        return costMicros;
    }

    public void setCostMicros(Long costMicros) {
        this.costMicros = costMicros;
    }
}
