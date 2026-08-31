package dev.gaurav.notification.worker.orchestrator;

import dev.gaurav.notification.domain.enums.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.TreeMap;

/**
 * Renders {@code template-code {k=v, k=v}} and nothing more.
 *
 * <p>TODO(phase-9): replace with the real template store and engine.
 *
 * <p>It does keep two behaviours the real renderer must also have, because they are the two that
 * change how the rest of the pipeline behaves and are cheap to get right now:
 *
 * <ul>
 *   <li>A missing template code throws {@link TemplateRenderingException}, so the
 *       {@code TEMPLATE_ERROR} → dead-letter path is exercised rather than theoretical.</li>
 *   <li>Variables are emitted in sorted order, so the rendered body is deterministic and the
 *       body-derived dedup hash is stable across JVMs. A {@code HashMap} iteration order here
 *       would produce a different hash on every pod for identical input, and business-level
 *       deduplication would silently stop working.</li>
 * </ul>
 */
public class EchoTemplateRenderer implements TemplateRenderer {

    private static final Logger log = LoggerFactory.getLogger(EchoTemplateRenderer.class);

    public EchoTemplateRenderer() {
        log.warn("no TemplateRenderer on the classpath: messages are rendered as an echo of the "
                + "template code and its variables");
    }

    @Override
    public RenderedMessage render(String templateCode, String locale, Channel channel,
                                  Map<String, String> data) {
        if (templateCode == null || templateCode.isBlank()) {
            throw new TemplateRenderingException("no templateCode on the request");
        }
        String variables = new TreeMap<>(data == null ? Map.<String, String>of() : data).toString();
        String body = templateCode + " " + variables;
        return new RenderedMessage(channel == Channel.EMAIL ? templateCode : null, body);
    }
}
