/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import io.airlift.log.Logger;
import io.rakam.presto.MemoryTracker;
import io.rakam.presto.MiddlewareConfig;
import io.rakam.presto.StreamWorkerContext;
import io.rakam.presto.TargetConnectorCommitter;

import javax.inject.Inject;

import java.util.concurrent.Executors;

import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

public class KinesisRecordProcessorFactory
        implements IRecordProcessorFactory
{
    private static final Logger log = Logger.get(KinesisRecordProcessorFactory.class);

    private final TargetConnectorCommitter committer;
    private final MiddlewareConfig middlewareConfig;
    private final StreamWorkerContext context;
    private final MemoryTracker memoryTracker;

    @Inject
    public KinesisRecordProcessorFactory(StreamWorkerContext context,
            MemoryTracker memoryTracker,
            MiddlewareConfig middlewareConfig, TargetConnectorCommitter committer)
    {
        this.context = context;
        this.middlewareConfig = middlewareConfig;
        this.memoryTracker = memoryTracker;
        this.committer = committer;

        if (log.isDebugEnabled()) {
            Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
                String message = format("[%s (%s%%) memory available]",
                        succinctBytes(memoryTracker.availableMemory()).toString(),
                        memoryTracker.availableMemoryInPercentage() * 100);
                log.debug(message);
            }, 5, 5, SECONDS);
        }
    }

    @Override
    public IRecordProcessor createProcessor()
    {
        return new KinesisRecordProcessor(context, middlewareConfig, memoryTracker, committer);
    }
}