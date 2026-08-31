package dev.gaurav.notification.application.command;

import dev.gaurav.notification.application.exception.ValidationException;

import java.util.List;

/**
 * Who to send to, in whichever of the four forms the caller chose.
 *
 * <p>The invariant enforced here is that exactly one form is populated. Both an inline list and a
 * manifest URI means two sources of truth for the audience, and the expander would have to guess
 * which one wins — the kind of ambiguity that shows up as "we mailed 10M people twice".
 *
 * @param declaredCount for the Claim Check forms this is the caller's declared size, echoed back in
 *                      the {@code 202} and reconciled after fan-out. It is deliberately trusted at
 *                      accept time: counting 10M lines out of S3 inside the request thread would
 *                      blow the 250 ms budget by three orders of magnitude
 */
public record RecipientSelector(
        RecipientKind kind, List<String> userIds, List<String> addresses, String ref, int declaredCount) {

    /** Above this an inline list is refused; the caller must use a manifest. */
    public static final int MAX_INLINE = 500;

    public RecipientSelector {
        if (kind == null) {
            throw ValidationException.invalidField("recipients.kind", "REQUIRED", "recipient kind is required");
        }
        userIds = userIds == null ? List.of() : List.copyOf(userIds);
        addresses = addresses == null ? List.of() : List.copyOf(addresses);
        ref = (ref == null || ref.isBlank()) ? null : ref;

        switch (kind) {
            case USER_IDS -> {
                requireInlineList("recipients.userIds", userIds, addresses, ref);
                declaredCount = userIds.size();
            }
            case INLINE -> {
                requireInlineList("recipients.addresses", addresses, userIds, ref);
                declaredCount = addresses.size();
            }
            case S3_MANIFEST, AUDIENCE_REF -> {
                if (ref == null) {
                    throw ValidationException.invalidField("recipients.uri", "REQUIRED",
                            kind + " requires a reference to the audience");
                }
                if (!userIds.isEmpty() || !addresses.isEmpty()) {
                    throw ValidationException.invalidField("recipients", "AMBIGUOUS",
                            "a claim-check audience cannot also carry an inline list");
                }
                if (declaredCount <= 0) {
                    throw ValidationException.invalidField("recipients.count", "REQUIRED",
                            "a declared count is required so the 202 can report progress "
                                    + "without reading the manifest first");
                }
            }
        }
    }

    public static RecipientSelector userIds(List<String> userIds) {
        return new RecipientSelector(RecipientKind.USER_IDS, userIds, List.of(), null, 0);
    }

    public static RecipientSelector inline(List<String> addresses) {
        return new RecipientSelector(RecipientKind.INLINE, List.of(), addresses, null, 0);
    }

    public static RecipientSelector manifest(String uri, int declaredCount) {
        return new RecipientSelector(RecipientKind.S3_MANIFEST, List.of(), List.of(), uri, declaredCount);
    }

    public static RecipientSelector audience(String audienceRef, int declaredCount) {
        return new RecipientSelector(RecipientKind.AUDIENCE_REF, List.of(), List.of(), audienceRef, declaredCount);
    }

    /** How many recipients we will charge quota for and report in the {@code 202}. */
    public int count() {
        return declaredCount;
    }

    /** True when the audience lives outside the request and is expanded asynchronously. */
    public boolean isClaimCheck() {
        return kind == RecipientKind.S3_MANIFEST || kind == RecipientKind.AUDIENCE_REF;
    }

    private static void requireInlineList(
            String field, List<String> present, List<String> otherList, String ref) {
        if (present.isEmpty()) {
            throw ValidationException.invalidField(field, "REQUIRED", "the recipient list is empty");
        }
        if (!otherList.isEmpty() || ref != null) {
            throw ValidationException.invalidField("recipients", "AMBIGUOUS",
                    "exactly one recipient form may be populated");
        }
        if (present.size() > MAX_INLINE) {
            throw ValidationException.invalidField(field, "TOO_MANY",
                    present.size() + " inline recipients exceeds " + MAX_INLINE + "; use S3_MANIFEST");
        }
    }
}
