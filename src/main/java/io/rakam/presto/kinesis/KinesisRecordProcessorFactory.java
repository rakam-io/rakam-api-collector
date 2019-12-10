/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import io.airlift.log.Logger;
import io.rakam.presto.MemoryTracker;
import io.rakam.presto.MiddlewareBuffer;
import io.rakam.presto.MiddlewareConfig;
import io.rakam.presto.StreamWorkerContext;
import io.rakam.presto.TargetConnectorCommitter;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

public class KinesisRecordProcessorFactory
        implements IRecordProcessorFactory
{
    private static final Logger log = Logger.get(KinesisRecordProcessorFactory.class);

    private final TargetConnectorCommitter committer;
    private final StreamWorkerContext context;
    private final MemoryTracker memoryTracker;
    private final MiddlewareBuffer middlewareBuffer;
    private ScheduledExecutorService scheduledExecutorService;

    @Inject
    public KinesisRecordProcessorFactory(StreamWorkerContext context,
            MemoryTracker memoryTracker,
            MiddlewareConfig middlewareConfig, TargetConnectorCommitter committer)
    {
        this.context = context;
        this.memoryTracker = memoryTracker;
        this.committer = committer;
        middlewareBuffer = new MiddlewareBuffer(middlewareConfig, memoryTracker);
    }

    @Override
    public IRecordProcessor createProcessor()
    {
        return new KinesisRecordProcessor(context, middlewareBuffer, memoryTracker, committer);
    }

    @PostConstruct
    public void start() {
        if (log.isInfoEnabled()) {
            scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                try {
                    long bytes = memoryTracker.availableMemory();
                    String message = format("[%s (%s%%) memory available] Active flush count is %d (%s), Middleware buffer is %s",
                            bytes > 0 ? succinctBytes(bytes).toString() : ("-" + succinctBytes(-bytes).toString()),
                            memoryTracker.availableMemoryInPercentage() * 100,
                            committer.getActiveFlushCount(),
                            committer.isFull() ? "full" : "not full",
                            middlewareBuffer.calculateSize().toString());
                    log.info(message);
                }
                catch (Exception e) {
                    log.debug(e, "Error while printing stats");
                }
            }, 30, 20, SECONDS);
        } else {
            scheduledExecutorService = null;
        }
    }

    @PreDestroy
    public void destroyWorkers() {
        if(scheduledExecutorService != null) {
            scheduledExecutorService.shutdown();
        }
    }
}