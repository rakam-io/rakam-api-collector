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
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.airlift.log.Logger;
import io.rakam.presto.BasicMemoryBuffer;
import io.rakam.presto.BatchRecords;
import io.rakam.presto.DatabaseHandler;
import io.rakam.presto.FieldNameConfig;
import io.rakam.presto.MemoryTracker;
import io.rakam.presto.MiddlewareBuffer;
import io.rakam.presto.MiddlewareConfig;
import io.rakam.presto.StreamWorkerContext;
import io.rakam.presto.TargetConnectorCommitter;
import io.rakam.presto.deserialization.DecoupleMessage;
import io.rakam.presto.deserialization.TableData;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class KinesisRecordProcessor
        implements IRecordProcessor
{
    private static final Logger log = Logger.get(KinesisRecordProcessor.class);

    private final TargetConnectorCommitter committer;
    private final BasicMemoryBuffer streamBuffer;
    private final MiddlewareBuffer middlewareBuffer;
    private final StreamWorkerContext context;
    private String shardId;

    public KinesisRecordProcessor(StreamWorkerContext context,
            MiddlewareConfig middlewareConfig,
            MemoryTracker memoryTracker,
            TargetConnectorCommitter committer)
    {
        this.committer = committer;
        this.context = context;
        this.streamBuffer = context.createBuffer();
        this.middlewareBuffer = new MiddlewareBuffer(middlewareConfig, memoryTracker);
    }

    @Override
    public void initialize(String shardId)
    {
        this.shardId = shardId;
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
            Map<SchemaTableName, TableData> pages = flushStream();

            middlewareBuffer.add(new BatchRecords(pages, () -> {
                try {
                    checkpointer.checkpoint();
                }
                catch (InvalidStateException | ShutdownException e) {
                    throw new RuntimeException(e);
                }
            }));

            Map<SchemaTableName, List<MiddlewareBuffer.TableCheckpoint>> list = middlewareBuffer.getRecordsToBeFlushed();
            if (!list.isEmpty()) {
                for (Map.Entry<SchemaTableName, List<MiddlewareBuffer.TableCheckpoint>> entry : list.entrySet()) {
                    List<MiddlewareBuffer.TableCheckpoint> checkpoints = entry.getValue();
                    committer.process(entry.getKey(), checkpoints).whenComplete((aVoid, throwable) -> {
                        if (throwable != null) {
                            log.error(throwable, "Error while processing records");
                        }

                        // TODO: What should we do if we can't isRecentData the data?
                        checkpoint(entry.getValue());
                    });
                }
            }
        }
    }

    public void checkpoint(List<MiddlewareBuffer.TableCheckpoint> value)
    {
        for (MiddlewareBuffer.TableCheckpoint tableCheckpoint : value) {
            try {
                tableCheckpoint.checkpoint();
            }
            catch (BatchRecords.CheckpointException e) {
                log.error(e, "Error while checkpointing records");
            }
        }
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer iRecordProcessorCheckpointer, ShutdownReason shutdownReason)
    {
        streamBuffer.clear();
        log.error("Shutdown %s, the reason is %s", shardId, shutdownReason.name());
    }

    private Map<SchemaTableName, TableData> flushStream()
    {
        Map<SchemaTableName, TableData> pages;
        try {
            BasicMemoryBuffer.Records list = streamBuffer.getRecords();
            pages = context.convert(list.buffer, list.bulkBuffer);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        streamBuffer.clear();
        return pages;
    }

    public static class KinesisDecoupleMessage
            implements DecoupleMessage<Record>
    {
        private final JsonFactory factory;
        private final String timeColumn;
        private final LoadingCache<String, Boolean> cache;

        public KinesisDecoupleMessage(DatabaseHandler handler, FieldNameConfig fieldNameConfig)
        {
            this.timeColumn = fieldNameConfig.getTimeField();
            factory = new ObjectMapper().getFactory();
            cache = CacheBuilder.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES)
                    .maximumSize(1000)
                    .build(new CacheLoader<String, Boolean>()
                    {
                        @Override
                        public Boolean load(String id)
                                throws Exception
                        {
                            return true;
                        }
                    });
        }

        @Override
        public void read(Record record, RecordData recordData)
                throws IOException
        {

        }
    }
}
