/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
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

    @Inject
    public KinesisRecordProcessorFactory(StreamWorkerContext context,
            MiddlewareConfig middlewareConfig, TargetConnectorCommitter committer)
    {
        this.context = context;
        this.middlewareConfig = middlewareConfig;
        this.committer = committer;
    }

    @Override
    public IRecordProcessor createProcessor()
    {
        return new KinesisRecordProcessor(context, middlewareConfig, committer);
    }
}