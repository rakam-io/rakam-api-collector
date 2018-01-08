/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import com.facebook.presto.raptor.storage.HistoricalDataConfig;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.rakam.presto.BasicMemoryBuffer;
import io.rakam.presto.BatchRecords;
import io.rakam.presto.MemoryTracker;
import io.rakam.presto.MiddlewareBuffer;
import io.rakam.presto.StreamWorkerContext;
import io.rakam.presto.TargetConnectorCommitter;
import io.rakam.presto.deserialization.TableData;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.rakam.presto.kafka.KafkaUtil.createConsumerConfig;
import static io.rakam.presto.kafka.KafkaUtil.findLatestOffsets;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class KafkaHistoricalWorker
{
    private static final String THREAD_NAME = "kafka-historical-worker";
    private static final Logger log = Logger.get(KafkaHistoricalWorker.class);

    private final MemoryTracker memoryTracker;
    private final HistoricalDataConfig historicalDataConfig;

    protected KafkaConsumer<byte[], byte[]> consumer;

    private final StreamWorkerContext<ConsumerRecord> context;
    protected KafkaConfig config;

    private final TargetConnectorCommitter committer;

    private Thread workerThread;
    private boolean working;

    private final CounterStat databaseFlushStats = new CounterStat();
    private final CounterStat recordStats = new CounterStat();
    private final CounterStat errorStats = new CounterStat();
    private final AtomicInteger activeFlushCount = new AtomicInteger();
    private final DistributionStat databaseFlushDistribution = new DistributionStat();

    private Map<Status, LongHolder> statusSpentTime = new HashMap<>();
    private long lastStatusChangeTime;
    private Status currentStatus;

    private final Map<Integer, Long> currentKafkaOffsets;
    private long lastPollInMillis;
    private final BasicMemoryBuffer buffer;

    @Inject
    public KafkaHistoricalWorker(KafkaConfig config, MemoryTracker memoryTracker, HistoricalDataConfig historicalDataConfig, StreamWorkerContext<ConsumerRecord> context, TargetConnectorCommitter committer)
    {
        this.config = config;
        this.context = context;
        this.committer = committer;
        this.historicalDataConfig = historicalDataConfig;
        this.memoryTracker = memoryTracker;
        working = true;
        currentKafkaOffsets = new HashMap<>();
        buffer = context.createBuffer();
    }

    @PreDestroy
    public void shutdown()
    {
        working = false;
    }

    @PostConstruct
    public void start()
    {
        if (config.getHistoricalDataTopic() == null) {
            log.warn("The config `kafka.historical-data-topic` is not set. Ignoring historical processing..");
            return;
        }
        workerThread = new Thread(this::execute);
        workerThread.setName(THREAD_NAME);
        workerThread.start();
        lastStatusChangeTime = System.currentTimeMillis();

        if (log.isDebugEnabled()) {
            Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
                String message = statusSpentTime.entrySet().stream().sorted(Comparator.comparingLong(o -> -o.getValue().value))
                        .map(entry -> entry.getKey().name() + ":" + Duration.succinctDuration(entry.getValue().value, MILLISECONDS).toString())
                        .collect(Collectors.joining(", "));
                log.debug(message);
            }, 30, 30, SECONDS);
        }
    }

    public void subscribe()
    {
        consumer = new KafkaConsumer(createConsumerConfig(config));
        consumer.subscribe(ImmutableList.of(config.getHistoricalDataTopic()));
        lastPollInMillis = System.currentTimeMillis();
    }

    private int getRecordsToBeProcessed()
    {
        int recordsToBeProcessed = 0;
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(consumer.assignment());
        for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
            Long currentOffset = currentKafkaOffsets.get(entry.getKey().partition());
            Long lastOffset = entry.getValue();

            long gap;
            if (currentOffset == null) {
                gap = lastOffset;
            }
            else {
                gap = lastOffset - currentOffset;
            }

            recordsToBeProcessed += gap;
        }
        return recordsToBeProcessed;
    }

    public void execute()
    {
        subscribe();

        try {
            // We need this for assignment
            ConsumerRecords<byte[], byte[]> firstPoll = consumer.poll(0);
            buffer.consumeRecords(firstPoll);

            while (working) {
                try {
                    changeType(Status.POLLING);

                    int recordsToBeProcessed = getRecordsToBeProcessed();
                    if (shouldFlush(recordsToBeProcessed)) {
                        ConsumerRecords<byte[], byte[]> records;
                        int numberOfEmptyResult = 0;
                        do {
                            records = consumer.poll(0);
                            buffer.consumeRecords(records);
                            if(records.count() == 0 && numberOfEmptyResult++ > 10) {
                                break;
                            }
                        } while (memoryTracker.availableMemory() - (buffer.getTotalBytes() * 2) > 0);

                        Optional<Queue<List<MiddlewareBuffer.TableCheckpoint>>> tableCheckpoints = flushDataSafe();
                        if (tableCheckpoints.isPresent()) {
                            List<MiddlewareBuffer.TableCheckpoint> poll = tableCheckpoints.get().poll();
                            if (poll != null) {
                                changeType(Status.CHECKPOINTING);
                            }
                            while (poll != null) {
                                checkpoint(poll);
                                poll = tableCheckpoints.get().poll();
                            }
                        }
                    }

                    try {
                        changeType(Status.SLEEPING);
                        SECONDS.sleep(15);
                    }
                    catch (InterruptedException e) {
                        break;
                    }
                }
                catch (Exception e) {
                    log.error(e, "Unexpected exception in Kafka historical records consumer thread.");
                }
            }
        }
        finally {
            KafkaUtil.close(context, consumer);
        }
    }

    private boolean shouldFlush(int recordsToBeProcessed)
    {
        return memoryTracker.availableMemoryInPercentage() > .3 &&
                ((System.currentTimeMillis() - lastPollInMillis) > historicalDataConfig.getMaxFlushDuration().toMillis()
                    || recordsToBeProcessed > historicalDataConfig.getMaxFlushRecords());
    }

    private Optional<Queue<List<MiddlewareBuffer.TableCheckpoint>>> flushDataSafe()
    {
        try {
            BasicMemoryBuffer<ConsumerRecord<byte[], byte[]>>.Records records = buffer.getRecords();

            changeType(Status.FLUSHING_MIDDLEWARE);

            long now = System.currentTimeMillis();
            log.debug("Flushing records (%s) from stream buffer, it's been %s since last flush.",
                    DataSize.succinctBytes(buffer.getTotalBytes()).toString(),
                    Duration.succinctDuration(now - buffer.getPreviousFlushTimeMillisecond(), MILLISECONDS).toString());

            Map<SchemaTableName, TableData> data = context.convert(records.buffer, records.bulkBuffer);

            Map<String, Int2LongOpenHashMap> latestOffsets = findLatestOffsets(records.buffer, records.bulkBuffer);

            buffer.clear();

            BatchRecords batchRecords = new BatchRecords(data, () -> commitSyncOffset(consumer, latestOffsets));

            Map<SchemaTableName, List<MiddlewareBuffer.TableCheckpoint>> map = new HashMap<>();
            for (Map.Entry<SchemaTableName, TableData> entry : data.entrySet()) {
                map.put(entry.getKey(), ImmutableList.of(new MiddlewareBuffer.TableCheckpoint(batchRecords, entry.getKey())));
            }

            long totalDataSize = data.entrySet().stream().mapToLong(e -> e.getValue().page.getRetainedSizeInBytes()).sum();
            log.debug("Flushed records to middleware buffer in %s, the data size is %s",
                    Duration.succinctDuration(System.currentTimeMillis() - now, MILLISECONDS).toString(),
                    DataSize.succinctBytes(totalDataSize));

            changeType(Status.FLUSHING_MIDDLEWARE);
            Queue<List<MiddlewareBuffer.TableCheckpoint>> checkpointQueue = new ArrayBlockingQueue<>(map.size());

            KafkaUtil.flush(map, committer, checkpointQueue, memoryTracker,
                    log, databaseFlushStats, databaseFlushDistribution, errorStats, activeFlushCount);

            return Optional.of(checkpointQueue);
        }
        catch (Throwable e) {
            log.error(e, "Error processing Kafka records, passing record to latest offset.");
            commitSyncOffset(consumer, null);
            return Optional.empty();
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

    public void commitSyncOffset(KafkaConsumer<byte[], byte[]> consumer, Map<String, Int2LongOpenHashMap> offsets)
    {
        KafkaUtil.commitSyncOffset(consumer, offsets);
    }

    private void changeType(Status status)
    {
        long now = System.currentTimeMillis();
        if (currentStatus != null) {
            statusSpentTime.computeIfAbsent(currentStatus, (k) -> new LongHolder()).value += now - lastStatusChangeTime;
        }

        currentStatus = status;
        this.lastStatusChangeTime = now;
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
        return activeFlushCount.get();
    }

    @Managed
    @Nested
    public CounterStat getRecordStats()
    {
        return recordStats;
    }

    @Managed
    @Nested
    public CounterStat getErrorStats()
    {
        return errorStats;
    }

    private static class LongHolder
    {
        long value;
    }

    private enum Status
    {
        POLLING, FLUSHING_MIDDLEWARE, CHECKPOINTING, SLEEPING, WAITING_FOR_MEMORY;
    }
}