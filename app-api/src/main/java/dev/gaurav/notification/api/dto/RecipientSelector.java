package dev.gaurav.notification.api.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.util.List;

/**
 * Who to send to, expressed as one of four mutually exclusive shapes.
 *
 * <p><strong>Why a discriminated union rather than four optional lists:</strong> the four kinds
 * have wildly different cost profiles and the accept path has to branch on them before it does
 * anything else. {@code INLINE} of five addresses is expanded synchronously; an
 * {@code S3_MANIFEST} of ten million is a claim check that must not be opened inside a request with
 * a 250 ms budget. Making the caller state which one they mean means the server never has to guess
 * from "which field is non-null", and a request that sets both {@code userIds} and {@code uri} is
 * rejected instead of silently resolved by field order.
 *
 * <p>{@code INLINE} is capped at 1 000 because that is roughly where a single accept transaction
 * stops fitting in the latency budget. Above it, the honest answer is a manifest.
 *
 * @param kind        which of the following fields is meaningful
 * @param inline      raw addresses; only for {@code INLINE}
 * @param userIds     platform user ids, resolved to addresses through the preference store
 * @param uri         {@code s3://bucket/key.ndjson} manifest; only for {@code S3_MANIFEST}
 * @param count       declared recipient count for a manifest, echoed back and reconciled after fan-out
 * @param audienceRef saved-segment identifier; only for {@code AUDIENCE_REF}
 */
public record RecipientSelector(

        @NotNull
        Kind kind,

        @Size(max = 1_000, message = "inline recipients are capped at 1000; use S3_MANIFEST above that")
        List<@Valid InlineRecipient> inline,

        @Size(max = 1_000, message = "userIds are capped at 1000; use S3_MANIFEST above that")
        List<@NotBlank @Size(max = 128) String> userIds,

        @Pattern(regexp = "^s3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]/.+$", message = "uri must be an s3:// object URI")
        String uri,

        @Positive
        Long count,

        @Size(max = 128)
        String audienceRef
) {

    /** The four ways a caller can name an audience. */
    public enum Kind {
        /** Addresses supplied in the request. No preference lookup by user id is possible. */
        INLINE,
        /** Platform user ids. Addresses and preferences are resolved server-side. */
        USER_IDS,
        /** A newline-delimited JSON object list in S3. Expanded asynchronously. */
        S3_MANIFEST,
        /** A saved segment, resolved by the audience service at expansion time. */
        AUDIENCE_REF
    }

    /**
     * One directly-addressed recipient.
     *
     * <p>{@code address} is deliberately not regex-validated per channel here. Phone-number and
     * e-mail validity are channel rules that belong with the channel adapter — a
     * {@code @Pattern} on e-mail in the DTO would reject valid RFC 5322 addresses and accept
     * unroutable ones, and would then have to be kept in sync with three providers' opinions.
     * The API's job is to bound the length; classification of a bad address is
     * {@code FailureType.INVALID_RECIPIENT}, reported per recipient rather than failing the batch.
     */
    public record InlineRecipient(
            @NotBlank @Size(max = 320) String address,
            @Size(max = 128) String userId,
            @Size(max = 35) String locale,
            @Size(max = 64) String timezone
    ) {
    }

    /**
     * Rejects a request that names one kind and populates another.
     *
     * <p>Reported as a single {@code recipients} violation rather than per field, because the
     * caller's mistake is one mistake.
     */
    @AssertTrue(message = "recipients must populate exactly the field its kind names")
    public boolean isConsistentWithKind() {
        if (kind == null) {
            return true; // @NotNull already reports this; a second violation adds noise, not signal.
        }
        return switch (kind) {
            case INLINE -> notEmpty(inline) && isEmpty(userIds) && uri == null && audienceRef == null;
            case USER_IDS -> notEmpty(userIds) && isEmpty(inline) && uri == null && audienceRef == null;
            case S3_MANIFEST -> uri != null && isEmpty(inline) && isEmpty(userIds) && audienceRef == null;
            case AUDIENCE_REF -> audienceRef != null && isEmpty(inline) && isEmpty(userIds) && uri == null;
        };
    }

    private static boolean isEmpty(List<?> list) {
        return list == null || list.isEmpty();
    }

    private static boolean notEmpty(List<?> list) {
        return list != null && !list.isEmpty();
    }
}
