package dev.gaurav.notification.scheduler.config;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.regex.Pattern;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Who this JVM is, for the two decisions in the scheduler that depend on it: which shards it owns
 * and what name it writes into a lease.
 *
 * <p><strong>The failure {@link #instanceId()} prevents.</strong> A lease owner recorded as the
 * pod name alone is not unique over time. A pod is OOM-killed while holding
 * {@code leader:notification-hydrator}, Kubernetes restarts it under the <em>same</em> hostname,
 * and the new process's compare-and-set renewal matches the dead process's value — so it silently
 * inherits a lease it never won, with no new fencing token. Appending a per-JVM UUID makes the
 * restarted process a different owner, which is what it is.
 *
 * <p>{@link #ordinal()} is read from the StatefulSet-style {@code -N} suffix on the pod name
 * because that is the one piece of cluster identity available without a Kubernetes API call. It is
 * an optimisation, not a requirement: {@link dev.gaurav.notification.scheduler.due.ShardAssignment}
 * falls back to a Redis membership registry, and if that is also unavailable the pod owns every
 * shard — slower, never incorrect.
 */
@Component
public class NodeIdentity {

    /** {@code notification-scheduler-2} → 2. Deployment pods have a random suffix and no match. */
    private static final Pattern ORDINAL_SUFFIX = Pattern.compile(".*-(\\d+)$");

    private final String nodeName;
    private final int ordinal;
    private final String instanceId;

    public NodeIdentity(@Value("${notification.scheduler.node-name:}") String configuredName) {
        this.nodeName = resolveName(configuredName);
        var matcher = ORDINAL_SUFFIX.matcher(this.nodeName);
        this.ordinal = matcher.matches() ? Integer.parseInt(matcher.group(1)) : -1;
        this.instanceId = this.nodeName + "/" + UUID.randomUUID();
    }

    /** The pod name — stable across restarts, and therefore <em>not</em> safe as a lease owner. */
    public String nodeName() {
        return nodeName;
    }

    /** The StatefulSet ordinal when the pod name carries one. */
    public OptionalInt ordinal() {
        return ordinal >= 0 ? OptionalInt.of(ordinal) : OptionalInt.empty();
    }

    /** Unique to this JVM run. The only value that may ever be written as a lock owner. */
    public String instanceId() {
        return instanceId;
    }

    private static String resolveName(String configured) {
        if (configured != null && !configured.isBlank()) {
            return configured;
        }
        var hostname = System.getenv("HOSTNAME");
        if (hostname != null && !hostname.isBlank()) {
            return hostname;
        }
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            // A machine with no resolvable hostname is a developer laptop on a captive network,
            // not a production pod. Falling back keeps the app bootable there.
            return "unknown-host";
        }
    }
}
