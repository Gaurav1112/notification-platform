package dev.gaurav.notification.application.fake;

import dev.gaurav.notification.application.port.DispatchTombstoneStore;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Records tombstones, or simulates a cache that is refusing writes. */
public final class FakeDispatchTombstoneStore implements DispatchTombstoneStore {

    private final List<UUID> tombstoned = new ArrayList<>();
    private boolean broken;

    /** Valkey unreachable: the write throws, and the caller must not undo the durable cancel. */
    public FakeDispatchTombstoneStore broken() {
        this.broken = true;
        return this;
    }

    @Override
    public void tombstone(String tenantId, UUID notificationId) {
        if (broken) {
            throw new IllegalStateException("valkey unreachable");
        }
        tombstoned.add(notificationId);
    }

    public List<UUID> tombstoned() {
        return List.copyOf(tombstoned);
    }
}
