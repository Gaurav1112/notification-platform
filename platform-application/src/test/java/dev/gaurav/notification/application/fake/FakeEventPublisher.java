package dev.gaurav.notification.application.fake;

import dev.gaurav.notification.application.port.EventPublisher;

import java.util.ArrayList;
import java.util.List;

/** Records what would have been published, and can simulate a broker that is refusing writes. */
public final class FakeEventPublisher implements EventPublisher {

    private final List<RequestedNotice> requested = new ArrayList<>();
    private final List<DispatchNotice> dispatched = new ArrayList<>();
    private final List<StatusNotice> statuses = new ArrayList<>();
    private boolean broken;

    /** Every publish now throws, as it would with every broker in the cluster unreachable. */
    public FakeEventPublisher broken() {
        this.broken = true;
        return this;
    }

    @Override
    public void publishRequested(RequestedNotice notice) {
        failIfBroken();
        requested.add(notice);
    }

    @Override
    public void publishDispatch(DispatchNotice notice) {
        failIfBroken();
        dispatched.add(notice);
    }

    @Override
    public void publishStatus(StatusNotice notice) {
        failIfBroken();
        statuses.add(notice);
    }

    public List<RequestedNotice> requested() {
        return List.copyOf(requested);
    }

    public List<DispatchNotice> dispatched() {
        return List.copyOf(dispatched);
    }

    public List<StatusNotice> statuses() {
        return List.copyOf(statuses);
    }

    private void failIfBroken() {
        if (broken) {
            throw new IllegalStateException("no brokers available");
        }
    }
}
