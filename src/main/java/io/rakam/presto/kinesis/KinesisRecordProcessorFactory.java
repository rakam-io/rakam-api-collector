/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import io.rakam.presto.MemoryTracker;
import io.rakam.presto.MiddlewareConfig;
import io.rakam.presto.StreamWorkerContext;
import io.rakam.presto.TargetConnectorCommitter;

import javax.inject.Inject;

public class KinesisRecordProcessorFactory
        implements IRecordProcessorFactory
{
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
    }

    @Override
    public IRecordProcessor createProcessor()
    {
        return new KinesisRecordProcessor(context, middlewareConfig, memoryTracker, committer);
    }
}