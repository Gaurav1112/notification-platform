/**
 * Fleet-wide rate limiting.
 *
 * <p>Deliberately not in-process: a per-JVM limiter set to the provider's published rate delivers
 * that rate multiplied by the pod count, and autoscaling raises the multiplier under load.
 */
package dev.gaurav.notification.resilience.ratelimit;
