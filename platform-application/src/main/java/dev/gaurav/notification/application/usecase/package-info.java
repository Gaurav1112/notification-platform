/**
 * The use cases — orchestration only.
 *
 * <p>Each class here reads as the sequence of decisions from
 * {@code docs/ARCHITECTURE.md} section 2, and nothing else: no SQL, no Kafka, no HTTP, no
 * retry loops. Everything that touches the outside world goes through
 * {@link dev.gaurav.notification.application.port}, which is what makes the accept path testable in
 * milliseconds with hand-written fakes rather than a Testcontainers stack.
 *
 * <p>The interesting content of this package is what is <em>absent</em> from the accept path:
 * preference resolution, template rendering, dedup and provider selection. Each is a dependency
 * that can be slow or down, and none of them should be able to make {@code POST /notifications}
 * fail.
 */
package dev.gaurav.notification.application.usecase;
