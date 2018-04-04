/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.rakam.presto.BasicMemoryBuffer;
import io.rakam.presto.BatchRecords;
import io.rakam.presto.FieldNameConfig;
import io.rakam.presto.HistoricalDataHandler;
import io.rakam.presto.MemoryTracker;
import io.rakam.presto.MiddlewareBuffer;
import io.rakam.presto.MiddlewareBuffer.TableCheckpoint;
import io.rakam.presto.MiddlewareConfig;
import io.rakam.presto.StreamWorkerContext;
import io.rakam.presto.TargetConnectorCommitter;
import io.rakam.presto.deserialization.DecoupleMessage;
import io.rakam.presto.deserialization.TableData;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntListIterator;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.time.LocalDate;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.airlift.units.DataSize.succinctBytes;
import static io.rakam.presto.kafka.KafkaUtil.createConsumerConfig;
import static io.rakam.presto.kafka.KafkaUtil.findLatestOffsets;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class KafkaRealTimeWorker
{
    private static final String THREAD_NAME = "kafka-realtime-worker";
    private static final Logger log = Logger.get(KafkaRealTimeWorker.class);
    private final DecoupleMessage<ConsumerRecord<byte[], byte[]>> decoupleMessage;
    private final HistoricalDataHandler historicalDataHandler;
    private final Predicate<String> whiteListCollections;
    private final StreamWorkerContext<ConsumerRecord<byte[], byte[]>> context;
    private final MemoryTracker memoryTracker;
    private final BasicMemoryBuffer<ConsumerRecord<byte[], byte[]>> buffer;
    private final MiddlewareBuffer middlewareBuffer;
    private final TargetConnectorCommitter committer;
    private final Queue<List<TableCheckpoint>> checkpointQueue;
    protected KafkaConsumer<byte[], byte[]> consumer;
    protected KafkaConfig config;
    private Thread workerThread;
    private boolean working;
    private CounterStat realTimeRecordsStats = new CounterStat();
    private CounterStat historicalRecordsStats = new CounterStat();
    private CounterStat databaseFlushStats = new CounterStat();
    private CounterStat errorStats = new CounterStat();
    private DistributionStat databaseFlushDistribution = new DistributionStat();
    private Map<Status, LongHolder> statusSpentTime = new HashMap<>();
    private long lastStatusChangeTime;
    private Status currentStatus;
    private int outdatedRecordIndex;

    @Inject
    public KafkaRealTimeWorker(KafkaConfig config, MemoryTracker memoryTracker, FieldNameConfig fieldNameConfig, Optional<HistoricalDataHandler> historicalDataHandler, DecoupleMessage decoupleMessage, MiddlewareConfig middlewareConfig, StreamWorkerContext<ConsumerRecord<byte[], byte[]>> context, TargetConnectorCommitter committer)
    {
        this.config = config;
        this.context = context;
        this.decoupleMessage = decoupleMessage;
        Set<String> whitelistedCollections = fieldNameConfig.getWhitelistedCollections();
        this.whiteListCollections = whitelistedCollections == null ? input -> true : input -> whitelistedCollections.contains(input);
        this.outdatedRecordIndex = config.getOutdatedDayIndex();
        this.historicalDataHandler = historicalDataHandler.orNull();
        this.committer = committer;
        this.memoryTracker = memoryTracker;
        this.middlewareBuffer = new MiddlewareBuffer(middlewareConfig, memoryTracker);
        buffer = context.createBuffer();
        working = true;
        checkpointQueue = new ConcurrentLinkedQueue<>();
    }

    @PreDestroy
    public void shutdown()
    {
        working = false;
    }

    @PostConstruct
    public void start()
    {
        workerThread = new Thread(this::execute);
        workerThread.setName(THREAD_NAME);
        workerThread.start();
        lastStatusChangeTime = System.currentTimeMillis();

        if (log.isDebugEnabled()) {
            Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
                String message = statusSpentTime.entrySet().stream().sorted(Comparator.comparingLong(o -> -o.getValue().value))
                        .map(entry -> entry.getKey().name() + ":" + Duration.succinctDuration(entry.getValue().value, MILLISECONDS).toString())
                        .collect(Collectors.joining(", "));
                message += format(" - in %s phase for %s ",
                        currentStatus.name(),
                        Duration.succinctDuration(System.currentTimeMillis() - lastStatusChangeTime, TimeUnit.MILLISECONDS).toString());
                message += format("[%s (%s%%) memory available]",
                        succinctBytes(memoryTracker.availableMemory()).toString(),
                        memoryTracker.availableMemoryInPercentage() * 100);
                log.debug(message);
            }, 5, 5, SECONDS);
        }
    }

    public void subscribe()
    {
        consumer = new KafkaConsumer(createConsumerConfig(config));
        consumer.subscribe(config.getTopic(), new ConsumerRebalanceListener()
        {

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions)
            {
                log.info("Revoked topicPartitions : {}", partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions)
            {
                for (TopicPartition tp : partitions) {
                    OffsetAndMetadata offsetAndMetaData = consumer.committed(tp);
                    long startOffset = offsetAndMetaData != null ? offsetAndMetaData.offset() : -1L;
                    log.info("Assigned topicPartition : %s offset : %s", tp, startOffset);
                }
            }
        });
    }

    public void execute()
    {
        subscribe();

        try {
            while (working) {
                try {
                    changeType(Status.POLLING);
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(0);

                    buffer.consumeRecords(records);
                    realTimeRecordsStats.update(records.count());

                    changeType(Status.FLUSHING_STREAM);
                    flushDataSafe();

                    if (buffer.shouldFlush()) {
                        changeType(Status.FLUSHING_STREAM);
                        flushDataSafe();
                    }

                    flushCheckpointQueue();

                    if (memoryTracker.availableMemoryInPercentage() < .30) {
                        changeType(Status.FLUSHING_STREAM);
                        flushDataSafe();

                        Set<TopicPartition> assignment = consumer.assignment();
                        consumer.pause(assignment);
                        while (memoryTracker.availableMemoryInPercentage() < .3) {
                            changeType(Status.WAITING_FOR_MEMORY);

                            try {
                                log.info("Not enough memory (%s)to process records sleeping for 1s", memoryTracker.availableMemoryInPercentage());
                                SECONDS.sleep(1);
                            }
                            catch (InterruptedException e) {
                                break;
                            }

                            flushCheckpointQueue();
                        }
                        consumer.resume(assignment);
                    }
                }
                catch (Throwable e) {
                    log.error(e, "Unexpected exception in Kafka real-time records consumer thread.");
                }
            }
        }
        finally {
            KafkaUtil.close(context, consumer);
        }
    }

    private void flushCheckpointQueue()
    {
        List<TableCheckpoint> poll = checkpointQueue.poll();
        if (poll != null) {
            changeType(Status.CHECKPOINTING);
        }
        while (poll != null) {
            for (TableCheckpoint tableCheckpoint : poll) {
                try {
                    tableCheckpoint.checkpoint();
                }
                catch (BatchRecords.CheckpointException e) {
                    log.error(e, "Error while checkpointing records");
                }
            }
            poll = checkpointQueue.poll();
        }
    }

    private void flushDataSafe()
    {
        try {
            BasicMemoryBuffer<ConsumerRecord<byte[], byte[]>>.Records records = buffer.getRecords();

            if (!records.bulkBuffer.isEmpty() || !records.buffer.isEmpty()) {
                changeType(Status.FLUSHING_MIDDLEWARE);

                long now = System.currentTimeMillis();
                log.debug("Flushing %s records of size (%s) from stream buffer, it's been %s since last flush.", buffer.getTotalRecords(),
                        DataSize.succinctBytes(buffer.getTotalBytes()).toString(),
                        Duration.succinctDuration(now - buffer.getPreviousFlushTimeMillisecond(), MILLISECONDS).toString());

                Map.Entry<Iterable<ConsumerRecord<byte[], byte[]>>, CompletableFuture<Void>> extractedData = extract(records);
                Iterable<ConsumerRecord<byte[], byte[]>> realTimeRecords = extractedData.getKey();
                CompletableFuture<Void> historicalDataAction = extractedData.getValue();

                Map<SchemaTableName, TableData> data = context.convert(realTimeRecords, records.bulkBuffer);

                Map<String, Int2LongOpenHashMap> latestOffsets = findLatestOffsets(records.buffer, records.bulkBuffer);

                buffer.clear();

                middlewareBuffer.add(new BatchRecords(data, historicalDataAction, () -> commitSyncOffset(consumer, latestOffsets)));

                long totalDataSize = data.entrySet().stream().mapToLong(e -> e.getValue().page.getRetainedSizeInBytes()).sum();
                log.debug("Flushed records to middleware buffer in %s, the data size is %s",
                        Duration.succinctDuration(System.currentTimeMillis() - now, MILLISECONDS).toString(),
                        succinctBytes(totalDataSize));
            }

            if (!committer.isFull()) {
                Map<SchemaTableName, List<TableCheckpoint>> map = middlewareBuffer.getRecordsToBeFlushed();
                if (!map.isEmpty()) {
                    Set<TopicPartition> assignment = consumer.assignment();
                    consumer.pause(assignment);

                    changeType(Status.FLUSHING_MIDDLEWARE);
                    KafkaUtil.flush(map, committer, checkpointQueue, memoryTracker,
                            log, databaseFlushStats, databaseFlushDistribution, realTimeRecordsStats, errorStats);

                    consumer.resume(assignment);
                }
            }
        }
        catch (Throwable e) {
            log.error(e, "Error processing Kafka records, passing record to latest offset.");
            commitSyncOffset(consumer, null);
        }
    }

    private Map.Entry<Iterable<ConsumerRecord<byte[], byte[]>>, CompletableFuture<Void>> extract(BasicMemoryBuffer<ConsumerRecord<byte[], byte[]>>.Records records)
    {
        CompletableFuture<Void> historicalDataAction = BatchRecords.COMPLETED_FUTURE;
        ProcessedRecords processedRecords = processRecords(records);
        int totalRecords = records.buffer.size();
        int historicalRecordCount = 0;
        Iterable<ConsumerRecord<byte[], byte[]>> realTimeRecords;
        if (processedRecords.recordsIndexedByDay.isEmpty()) {
            realTimeRecords = () -> Iterators.filter(records.buffer.iterator(), new BitMapRecordPredicate(processedRecords.bitmapForRecords));
            totalRecords = processedRecords.realTimeRecordCount;
            historicalDataAction = BatchRecords.COMPLETED_FUTURE;
            realTimeRecordsStats.update(totalRecords);
            historicalRecordsStats.update(0);
        }
        else {
            realTimeRecords = () -> Iterators.filter(records.buffer.iterator(), new BitMapRecordPredicate(processedRecords.bitmapForRecords));

            historicalRecordCount = totalRecords - processedRecords.realTimeRecordCount;

            Iterable<ConsumerRecord<byte[], byte[]>> filter = () -> Iterators.filter(records.buffer.iterator(), new NegateBitMapRecordPredicate(processedRecords.bitmapForRecords));
            changeType(Status.FLUSHING_HISTORICAL);

            if (historicalDataHandler != null) {
                historicalDataAction = historicalDataHandler.handle(filter, historicalRecordCount);
            }

            changeType(Status.FLUSHING_MIDDLEWARE);

            realTimeRecordsStats.update(processedRecords.realTimeRecordCount);
            historicalRecordsStats.update(historicalRecordCount);
        }

        log.info("realTimeRecords: " + processedRecords.realTimeRecordCount + " historicalRecordCount: " + historicalRecordCount);
        return new SimpleImmutableEntry<>(realTimeRecords, historicalDataAction);
    }

    private ProcessedRecords processRecords(BasicMemoryBuffer<ConsumerRecord<byte[], byte[]>>.Records records)
    {
        Int2ObjectArrayMap<IntArrayList> recordsIndexedByDay = new Int2ObjectArrayMap<>();
        int todayInDate = Ints.checkedCast(LocalDate.now().toEpochDay());
        DecoupleMessage.RecordData recordData = new DecoupleMessage.RecordData();
        int realtimeRecordCount = 0;
        boolean[] bitmapForRecords = new boolean[records.buffer.size()];
        for (int i = 0; i < records.buffer.size(); i++) {
            ConsumerRecord<byte[], byte[]> record = records.buffer.get(i);

            int dayOfRecord;
            String collection;
            try {
                decoupleMessage.read(record, recordData);
                dayOfRecord = recordData.date;
                collection = recordData.collection;
            }
            catch (Throwable e) {
                log.error(e, "Error while parsing record");
                continue;
            }

            if (!whiteListCollections.apply(collection)) {
                continue;
            }

            if (historicalDataHandler == null || (dayOfRecord >= (todayInDate - outdatedRecordIndex) && dayOfRecord <= todayInDate)) {
                bitmapForRecords[i] = true;
                realtimeRecordCount++;
            }
            else {
                IntArrayList list = recordsIndexedByDay.get(dayOfRecord);
                if (list == null) {
                    list = new IntArrayList();
                    recordsIndexedByDay.put(dayOfRecord, list);
                }
                list.add(i);
            }
        }

        if (config.getHistoricalWorkerEnabled()) {
            for (Int2ObjectMap.Entry<IntArrayList> entry : recordsIndexedByDay.int2ObjectEntrySet()) {
                int day = entry.getIntKey();
                IntArrayList recordIndexes = entry.getValue();
                if (recordIndexes.size() > 1000 && (recordIndexes.size() * 100.0 / records.buffer.size()) > 25) {
                    IntListIterator iterator = recordIndexes.iterator();
                    while (iterator.hasNext()) {
                        int i = iterator.nextInt();
                        bitmapForRecords[i] = true;
                        realtimeRecordCount++;
                    }
                    recordsIndexedByDay.remove(day);
                }
            }
        }

        return new ProcessedRecords(recordsIndexedByDay, bitmapForRecords, realtimeRecordCount);
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
        lastStatusChangeTime = now;
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

    @Managed
    @Nested
    public CounterStat getHistoricalRecordsStats()
    {
        return historicalRecordsStats;
    }

    @Managed
    @Nested
    public CounterStat getErrorStats()
    {
        return errorStats;
    }

    @Managed
    @Nested
    public Map<Status, LongHolder> getStatusSpentTime()
    {
        return statusSpentTime;
    }

    private enum Status
    {
        POLLING, FLUSHING_STREAM, FLUSHING_MIDDLEWARE, CHECKPOINTING, FLUSHING_HISTORICAL, WAITING_FOR_MEMORY;
    }

    private static class ProcessedRecords
    {
        public final Int2ObjectArrayMap<IntArrayList> recordsIndexedByDay;
        public final boolean[] bitmapForRecords;
        public final int realTimeRecordCount;

        public ProcessedRecords(Int2ObjectArrayMap<IntArrayList> recordsIndexedByDay, boolean[] bitmapForRecords, int realTimeRecordCount)
        {
            this.recordsIndexedByDay = recordsIndexedByDay;
            this.bitmapForRecords = bitmapForRecords;
            this.realTimeRecordCount = realTimeRecordCount;
        }
    }

    private static class NegateBitMapRecordPredicate
            implements com.google.common.base.Predicate<ConsumerRecord<byte[], byte[]>>
    {
        private final boolean[] bitmapForRecords;
        private int i;

        public NegateBitMapRecordPredicate(boolean[] bitmapForRecords)
        {
            this.bitmapForRecords = bitmapForRecords;
        }

        @Override
        public boolean apply(@Nullable ConsumerRecord<byte[], byte[]> input)
        {
            return !bitmapForRecords[i++];
        }
    }

    private static class BitMapRecordPredicate
            implements com.google.common.base.Predicate<ConsumerRecord<byte[], byte[]>>
    {
        private final boolean[] bitmapForRecords;
        private int i;

        public BitMapRecordPredicate(boolean[] bitmapForRecords)
        {
            this.bitmapForRecords = bitmapForRecords;
        }

        @Override
        public boolean apply(@Nullable ConsumerRecord<byte[], byte[]> input)
        {
            return bitmapForRecords[i++];
        }
    }

    private static class LongHolder
    {
        long value;
    }
}
