/**
 * Outbound ports — the interfaces the use cases call and the infrastructure modules implement.
 *
 * <p>Every type here is expressed in domain and JDK terms only. No {@code KafkaTemplate}, no
 * {@code EntityManager}, no {@code RedisTemplate}. That is what lets the accept path be unit-tested
 * in milliseconds with hand-written fakes, and it is what stops a Kafka upgrade from turning into a
 * change to business logic.
 *
 * <p>Direction matters: the application layer <em>owns</em> these interfaces. The adapters depend on
 * this package, never the reverse.
 */
package dev.gaurav.notification.application.port;
