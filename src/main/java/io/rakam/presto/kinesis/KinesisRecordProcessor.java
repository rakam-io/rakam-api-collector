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
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import io.airlift.units.Duration;
import io.rakam.presto.BasicMemoryBuffer;
import io.rakam.presto.BatchRecords;
import io.rakam.presto.MemoryTracker;
import io.rakam.presto.MiddlewareBuffer;
import io.rakam.presto.MiddlewareBuffer.TableCheckpoint;
import io.rakam.presto.StreamWorkerContext;
import io.rakam.presto.TargetConnectorCommitter;
import io.rakam.presto.deserialization.TableData;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.succinctDuration;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class KinesisRecordProcessor
        implements IRecordProcessor
{
    private static final Logger log = Logger.get(KinesisRecordProcessor.class);

    private final TargetConnectorCommitter committer;
    private final BasicMemoryBuffer streamBuffer;
    private final MiddlewareBuffer middlewareBuffer;
    private final StreamWorkerContext context;
    private final MemoryTracker memoryTracker;
    private CounterStat errorStats = new CounterStat();
    private CounterStat realTimeRecordsStats = new CounterStat();
    private CounterStat databaseFlushStats = new CounterStat();
    private DistributionStat databaseFlushDistribution = new DistributionStat();

    private String shardId;

    public KinesisRecordProcessor(StreamWorkerContext context,
            MiddlewareBuffer middlewareBuffer,
            MemoryTracker memoryTracker,
            TargetConnectorCommitter committer)
    {
        this.committer = committer;
        this.context = context;
        this.streamBuffer = context.createBuffer();
        this.memoryTracker = memoryTracker;
        this.middlewareBuffer = middlewareBuffer;
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
                    streamBuffer.consumeRecord(record);
                    break;
                case 1:
                    log.error("Found previous version of bulk format");
                    break;
                case 3:
                    long actualDataSize = data.getLong(1);
                    streamBuffer.consumeBatch(record, actualDataSize);
                    break;
                default:
                    log.warn("Invalid record. ignoring..");
                    continue;
            }
        }

        if (streamBuffer.shouldFlush()) {
            flushDataSafe(checkpointer);
        }

        while (memoryTracker.availableMemoryInPercentage() < .2) {
            try {
                log.info("Not enough memory (%s) to process records, sleeping for 3s..",
                        memoryTracker.availableMemoryInPercentage());
                SECONDS.sleep(3);
            }
            catch (InterruptedException e) {
                break;
            }

            flushDataSafe(checkpointer);
        }
    }

    private void flushDataSafe(IRecordProcessorCheckpointer checkpointer)
    {
        Map<SchemaTableName, TableData> pages = flushStream();

        if (!pages.isEmpty()) {
            middlewareBuffer.add(new BatchRecords(pages, () -> {
                try {
                    checkpointer.checkpoint();
                    log.info("Checkpoint executed for %d records", pages.values().stream().mapToLong(e -> e.page.getPositionCount()).sum());
                }
                catch (InvalidStateException | ShutdownException e) {
                    log.error(e);
                    throw new RuntimeException(e);
                }
            }));
        }

        if (!committer.isFull()) {
            Map<SchemaTableName, List<TableCheckpoint>> map = middlewareBuffer.getRecordsToBeFlushed();
            if (!map.isEmpty()) {
                for (Map.Entry<SchemaTableName, List<TableCheckpoint>> entry : map.entrySet()) {
                    long now = System.currentTimeMillis();
                    SchemaTableName table = entry.getKey();
                    List<TableCheckpoint> records = entry.getValue();
                    CompletableFuture<Void> dbWriteWork = committer.process(table, records);

                    dbWriteWork.whenComplete((aVoid, throwable) -> {
                        long totalRecordCount = records.stream()
                                .mapToLong(e -> e.getTable().page.getPositionCount())
                                .sum();

                        long totalDataSize = records.stream()
                                .mapToLong(e -> e.getTable().page.getRetainedSizeInBytes())
                                .sum();
                        Duration totalDuration = succinctDuration(System.currentTimeMillis() - now, MILLISECONDS);
                        if (throwable != null) {
                            errorStats.update(totalRecordCount);
                            log.error(throwable, "Error while processing records for collection %s", table.toString());

                            double count = realTimeRecordsStats.getFiveMinute().getCount();
                            if (count > 100 && (errorStats.getFiveMinute().getCount() / count) > .4) {
                                log.error("The maximum error threshold is reached. Exiting the program...");
                                Runtime.getRuntime().exit(1);
                            }
                        }
                        else {
                            log.debug("Saved data in buffer (%s - %d records) for collection %s in %s.",
                                    succinctBytes(totalDataSize).toString(), totalRecordCount,
                                    table.toString(),
                                    totalDuration.toString());
                        }

                        checkpoint(records);

                        databaseFlushStats.update(totalRecordCount);
                        databaseFlushDistribution.add(totalDuration.toMillis());
                        memoryTracker.freeMemory(totalDataSize);
                    });
                }
            }
        }
    }

    public void checkpoint(List<TableCheckpoint> value)
    {
        for (TableCheckpoint tableCheckpoint : value) {
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
        streamBuffer.shutdown();
        if(shutdownReason == ShutdownReason.TERMINATE) {
            try {
                iRecordProcessorCheckpointer.checkpoint();
            }
            catch (InvalidStateException|ShutdownException e) {
                log.error(e, "Shutdown %s, the reason is %s", shardId, shutdownReason.name());

            }
        }

        log.info("Shutdown %s, the reason is %s", shardId, shutdownReason.name());
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

    @Managed
    @Nested
    public CounterStat getRealTimeRecordsStats()
    {
        return realTimeRecordsStats;
    }

    @Managed
    @Nested
    public DistributionStat getDatabaseFlushDistribution()
    {
        return databaseFlushDistribution;
    }

    @Managed
    @Nested
    public CounterStat getDatabaseFlushStats()
    {
        return databaseFlushStats;
    }

    @Managed
    public int getActiveFlushCount()
    {
        return committer.getActiveFlushCount();
    }

//    @Managed
//    @Nested
//    public CounterStat getHistoricalRecordsStats()
//    {
//        return historicalRecordsStats;
//    }

    @Managed
    @Nested
    public CounterStat getErrorStats()
    {
        return errorStats;
    }
}
