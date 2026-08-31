package dev.gaurav.notification.application.fake;

import dev.gaurav.notification.application.port.NotificationQuery;
import dev.gaurav.notification.application.result.DeliveryAttemptView;
import dev.gaurav.notification.application.result.NotificationStatusView;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * A read side keyed by {@code tenantId|id}, exactly as the real query filters.
 *
 * <p>Keying on the pair rather than on the id alone is deliberate: it means a test asking for
 * another tenant's notification gets the same empty answer the database would give, instead of the
 * row plus a check the fake forgot to make.
 */
public final class FakeNotificationQuery implements NotificationQuery {

    private final Map<String, NotificationStatusView> statuses = new HashMap<>();
    private final Map<String, List<DeliveryAttemptView>> attempts = new HashMap<>();
    private final List<Integer> requestedLimits = new ArrayList<>();

    public FakeNotificationQuery with(String tenantId, NotificationStatusView view) {
        statuses.put(key(tenantId, view.id()), view);
        return this;
    }

    public FakeNotificationQuery withAttempts(
            String tenantId, UUID notificationId, List<DeliveryAttemptView> views) {
        attempts.put(key(tenantId, notificationId), List.copyOf(views));
        return this;
    }

    @Override
    public Optional<NotificationStatusView> findStatus(String tenantId, UUID notificationId) {
        return Optional.ofNullable(statuses.get(key(tenantId, notificationId)));
    }

    @Override
    public List<DeliveryAttemptView> findAttempts(String tenantId, UUID notificationId, int limit) {
        requestedLimits.add(limit);
        var all = attempts.getOrDefault(key(tenantId, notificationId), List.of());
        return all.subList(0, Math.min(limit, all.size()));
    }

    /** The limits the use case actually passed down — the clamp is only real if it arrives here. */
    public List<Integer> requestedLimits() {
        return List.copyOf(requestedLimits);
    }

    private static String key(String tenantId, UUID id) {
        return tenantId + '|' + id;
    }
}
