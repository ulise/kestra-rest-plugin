package io.kestra.plugin.restserver;

import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.storages.StorageContext;
import io.kestra.core.storages.StorageInterface;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.net.URI;

/**
 * Writes what a request carries — its uploaded file parts, and its body when the route stores it — into Kestra's
 * internal storage, under the execution the request is about to create.
 * <p>
 * Storing under the execution is what makes the files someone else's problem to clean up: they are purged with the
 * execution, exactly like a task's output files. It also means the execution id has to exist before a single byte is
 * read, which is why {@link RestServerRealtimeTrigger} mints it up front instead of letting
 * {@code TriggerService.generateRealtimeExecution} do it.
 */
class RequestStorage {

    /**
     * Subdirectory of the execution's storage context holding everything a request brought with it. Matches the
     * layout Kestra core uses for its own webhook trigger.
     */
    private static final String WEBHOOK_DIRECTORY = "webhook";

    /**
     * File name a stored body is written under. A request carries one body and it has no name of its own, so it
     * needs neither a number nor anything the caller chose.
     */
    private static final String BODY_FILE = "body";

    private final StorageInterface storage;
    private final String tenantId;
    private final String namespace;
    private final String flowId;
    private final Logger logger;

    private RequestStorage(StorageInterface storage, String tenantId, String namespace, String flowId, Logger logger) {
        this.storage = storage;
        this.tenantId = tenantId;
        this.namespace = namespace;
        this.flowId = flowId;
        this.logger = logger;
    }

    /**
     * Resolves the internal storage from the running Kestra instance. Done once when the trigger starts rather than
     * per request, and eagerly rather than on the first upload, so a misconfigured instance fails the trigger
     * instead of failing one caller's request halfway through.
     */
    @SuppressWarnings("removal")
    static RequestStorage of(RunContext runContext, TriggerContext triggerContext) {
        if (!(runContext instanceof DefaultRunContext defaultRunContext)) {
            throw new IllegalStateException(
                "The REST server trigger requires the standard Kestra runtime to store request files; got "
                    + runContext.getClass().getName()
            );
        }

        return new RequestStorage(
            defaultRunContext.getApplicationContext().getBean(StorageInterface.class),
            triggerContext.getTenantId(),
            triggerContext.getNamespace(),
            triggerContext.getFlowId(),
            runContext.logger()
        );
    }

    /**
     * Streams one uploaded file part into the storage, returning the URI the flow reaches it by.
     *
     * @param index position of the part in the request, which keeps two parts apart when the caller gives them the
     *              same filename
     */
    URI storePart(String executionId, int index, String filename, InputStream content) throws IOException {
        URI uri = URI.create("%s/%s/%d/%s".formatted(
            executionUri(executionId),
            WEBHOOK_DIRECTORY,
            index,
            fileName(filename)
        ));

        return storage.put(tenantId, namespace, uri, content);
    }

    /**
     * Streams the request body into the storage, returning the URI the flow reaches it by, or {@code null} when the
     * request has no body at all — which must not leave an empty file behind.
     */
    URI storeBody(String executionId, InputStream content) throws IOException {
        try (PushbackInputStream body = new PushbackInputStream(content)) {
            int first = body.read();
            if (first == -1) {
                return null;
            }
            body.unread(first);

            URI uri = URI.create("%s/%s/%s".formatted(executionUri(executionId), WEBHOOK_DIRECTORY, BODY_FILE));

            return storage.put(tenantId, namespace, uri, body);
        }
    }

    /**
     * Deletes everything stored for a request that ends without creating an execution. Nothing else would ever purge
     * it, as the execution the files are scoped to will not exist. Best-effort: a request that already failed should
     * not fail differently because the cleanup did too.
     */
    void deleteStored(String executionId) {
        URI prefix = URI.create("%s/%s".formatted(executionUri(executionId), WEBHOOK_DIRECTORY));

        try {
            storage.deleteByPrefix(tenantId, namespace, prefix);
        } catch (IOException | RuntimeException e) {
            logger.warn("Unable to delete the files stored for the abandoned execution {}", executionId, e);
        }
    }

    private URI executionUri(String executionId) {
        return StorageContext
            .forExecution(tenantId, namespace, flowId, executionId)
            .getContextStorageURI();
    }

    /**
     * Keeps only the file name of a caller-supplied filename, so a part called {@code ../../evil.jpg} cannot be
     * written outside its execution's directory. A part that arrives without a usable name still gets stored, under
     * a generic one, rather than being dropped.
     */
    static String fileName(String filename) {
        if (filename == null) {
            return BODY_FILE;
        }

        // Both separators are stripped: the name is chosen by the caller, not by this filesystem.
        String name = filename.substring(Math.max(filename.lastIndexOf('/'), filename.lastIndexOf('\\')) + 1).strip();

        return name.isEmpty() || ".".equals(name) || "..".equals(name) ? BODY_FILE : name;
    }
}
