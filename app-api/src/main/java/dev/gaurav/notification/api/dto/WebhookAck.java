package dev.gaurav.notification.api.dto;

/**
 * The only body an inbound provider webhook ever receives.
 *
 * <p><strong>A duplicate returns 200, not 409.</strong> Providers treat any non-2xx as "retry", and
 * several of them retry with escalating backoff for hours. Answering 409 to a redelivery — which is
 * normal, expected traffic under at-least-once semantics — converts a duplicate into an endless
 * retry loop that ends with the provider disabling the endpoint. The {@code duplicate} flag exists
 * for our own tests and dashboards; the status code stays 200 either way.
 */
public record WebhookAck(String status, boolean duplicate) {

    public static WebhookAck accepted() {
        return new WebhookAck("accepted", false);
    }

    public static WebhookAck alreadySeen() {
        return new WebhookAck("accepted", true);
    }
}
