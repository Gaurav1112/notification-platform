/**
 * The driven adapters that satisfy the application's Kafka- and JSON-shaped outbound ports.
 *
 * <p>Placed under {@code dev.gaurav.notification.adapter.messaging} rather than inside
 * {@code dev.gaurav.notification.messaging} for the same reason as the persistence adapters: the
 * platform modules' own test contexts scan their own root package, and an adapter that needs a
 * collaborator from a different module has no business appearing in them. The three deployables
 * scan {@code dev.gaurav.notification} and pick this package up.
 */
package dev.gaurav.notification.adapter.messaging;
