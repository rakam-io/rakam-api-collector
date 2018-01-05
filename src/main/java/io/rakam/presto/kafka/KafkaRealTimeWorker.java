/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.rakam.presto.*;
import io.rakam.presto.MiddlewareBuffer.TableCheckpoint;
import io.rakam.presto.deserialization.TableData;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.rakam.presto.kafka.KafkaUtil.createConsumerConfig;
import static io.rakam.presto.kafka.KafkaUtil.findLatestRecord;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class KafkaRealTimeWorker
{
    private static final Logger log = Logger.get(KafkaRealTimeWorker.class);

    protected KafkaConsumer<byte[], byte[]> consumer;

    private final StreamWorkerContext<ConsumerRecord> context;
    private final MemoryTracker memoryTracker;
    protected KafkaConfig config;

    private final BasicMemoryBuffer<ConsumerRecord<byte[], byte[]>> buffer;
    private final MiddlewareBuffer middlewareBuffer;

    private final TargetConnectorCommitter committer;
    private final Queue<List<TableCheckpoint>> checkpointQueue;
    private ExecutorService executor;

    private Thread workerThread;
    private AtomicBoolean working;

    @Inject
    public KafkaRealTimeWorker(KafkaConfig config, MemoryTracker memoryTracker, MiddlewareConfig middlewareConfig, StreamWorkerContext<ConsumerRecord> context, TargetConnectorCommitter committer)
    {
        this.config = config;
        this.context = context;
        this.committer = committer;
        this.memoryTracker = memoryTracker;
        this.middlewareBuffer = new MiddlewareBuffer(middlewareConfig, memoryTracker);
        buffer = context.createBuffer();
        this.executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                .setNameFormat("kafka-topic-consumer").build());
        working = new AtomicBoolean(true);
        checkpointQueue = new ConcurrentLinkedQueue<>();
    }

    @PreDestroy
    public void shutdown()
    {
        working.set(false);
    }

    @PostConstruct
    public void start()
    {
        workerThread = new Thread(this::execute);
        workerThread.setName("kafka-realtime-worker");
        workerThread.start();
    }

    public void subscribe()
    {
        consumer = new KafkaConsumer(createConsumerConfig(config));
        consumer.subscribe(config.getTopic());
    }

    public void execute()
    {
        subscribe();

        try {
            while (working.get()) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(0);

                buffer.consumeRecords(records);

                if (buffer.shouldFlush()) {
                    flushDataSafe();
                }

                List<TableCheckpoint> poll = checkpointQueue.poll();
                while (poll != null) {
                    checkpoint(poll);
                    poll = checkpointQueue.poll();
                }

                while (memoryTracker.availableMemory() < 0) {
                    flushDataSafe();

                    try {
                        MILLISECONDS.sleep(200);
                    }
                    catch (InterruptedException e) {
                        break;
                    }
                }
            }
        }
        finally {
            KafkaUtil.close(context, consumer, executor, log);
        }
    }

    private void flushDataSafe()
    {
        try {
            BasicMemoryBuffer<ConsumerRecord<byte[], byte[]>>.Records records = buffer.getRecords();

            if (!records.bulkBuffer.isEmpty() || !records.buffer.isEmpty()) {
                long now = System.currentTimeMillis();
                log.debug("Flushing records (%s) from stream buffer, it's been %s since last flush.",
                        DataSize.succinctBytes(buffer.getTotalBytes()).toString(),
                        Duration.succinctDuration(now - buffer.getPreviousFlushTimeMillisecond(), MILLISECONDS).toString());

                Map<SchemaTableName, TableData> data = context.convert(records.buffer, records.bulkBuffer, ImmutableList.of());

                buffer.clear();

                middlewareBuffer.add(new BatchRecords(data, () -> commitSyncOffset(consumer, findLatestRecord(records))));

                long totalDataSize = data.entrySet().stream().mapToLong(e -> e.getValue().page.getRetainedSizeInBytes()).sum();
                log.debug("Flushed records to middleware buffer in %s, the data size is %s",
                        Duration.succinctDuration(System.currentTimeMillis() - now, MILLISECONDS).toString(),
                        DataSize.succinctBytes(totalDataSize));
            }

            KafkaUtil.flushIfNeeded(middlewareBuffer, committer, checkpointQueue, memoryTracker, log);
        }
        catch (Throwable e) {
            log.error(e, "Error processing Kafka records, passing record to latest offset.");
            commitSyncOffset(consumer, null);
        }
    }

    @SuppressWarnings("Duplicates")
    public void commitSyncOffset(KafkaConsumer<byte[], byte[]> consumer, ConsumerRecord record)
    {
        if (record == null) {
            consumer.commitSync();
        }
        else {
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
            consumer.commitSync(offsets);
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
}
