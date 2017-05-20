/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;
import io.airlift.log.Logger;
import io.rakam.presto.BasicMemoryBuffer;
import io.rakam.presto.BatchRecords;
import io.rakam.presto.MessageEventTransformer;
import io.rakam.presto.MiddlewareBuffer;
import io.rakam.presto.MiddlewareConfig;
import io.rakam.presto.StreamWorkerContext;
import io.rakam.presto.TargetConnectorCommitter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class KinesisRecordProcessor
        implements IRecordProcessor
{
    private static final Logger log = Logger.get(KinesisRecordProcessor.class);

    private final TargetConnectorCommitter committer;
    private final BasicMemoryBuffer streamBuffer;
    private final MiddlewareBuffer middlewareBuffer;
    private final StreamWorkerContext context;

    public KinesisRecordProcessor(StreamWorkerContext context,
            MiddlewareConfig middlewareConfig,
            TargetConnectorCommitter committer)
    {
        this.committer = committer;
        this.context = context;
        this.streamBuffer = context.createBuffer();
        this.middlewareBuffer = new MiddlewareBuffer(middlewareConfig);
    }

    @Override
    public void initialize(String shardId)
    {
        log.info("Kinesis consumer shard %s initialized", shardId);
    }

    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer)
    {
        for (Record record : records) {
            ByteBuffer data = record.getData();
            byte type = data.get(0);
            switch (type) {
                case 0:
                case 2:
                    streamBuffer.consumeRecord(record, data.remaining());
                    break;
                case 1:
                    long length = data.getLong(1);
                    streamBuffer.consumeBatch(record, length);
                    break;
                default:
                    log.warn("Invalid record. ignoring..");
                    continue;
            }
        }

        if (streamBuffer.shouldFlush()) {
            Table<String, String, MessageEventTransformer.TableData> pages = flushStream();

            middlewareBuffer.add(new BatchRecords(pages, () -> {
                try {
                    checkpointer.checkpoint();
                }
                catch (InvalidStateException | ShutdownException e) {
                    throw Throwables.propagate(e);
                }
            }));

            if (middlewareBuffer.shouldFlush()) {
                List<BatchRecords> list = middlewareBuffer.flush();
                if (!list.isEmpty()) {
                    committer.process(Iterables.transform(list, BatchRecords::getTable));

                    list.forEach(l -> {
                        try {
                            l.checkpoint();
                        }
                        catch (BatchRecords.CheckpointException e) {
                            log.error(e, "Error while checkpointing records");
                        }
                    });
                }
            }
        }
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer iRecordProcessorCheckpointer, ShutdownReason shutdownReason)
    {
        streamBuffer.clear();
    }

    private Table<String, String, MessageEventTransformer.TableData> flushStream()
    {
        Table<String, String, MessageEventTransformer.TableData> pages;
        try {
            Map.Entry<List, List> list = streamBuffer.getRecords();
            pages = context.convert(list.getKey(), list.getValue());
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        streamBuffer.clear();
        return pages;
    }
}
