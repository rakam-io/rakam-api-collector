package io.rakam.presto.kafka;

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.log.Logger;
import io.rakam.presto.*;
import io.rakam.presto.deserialization.TableData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.rakam.presto.kafka.KafkaHistoricalDataHandler.KAFKA_KEY_FOR_HISTORICAL_DATA;
import static io.rakam.presto.kafka.KafkaUtil.*;
import static java.util.concurrent.TimeUnit.SECONDS;

public class KafkaHistoricalWorker {
    private static final Logger log = Logger.get(KafkaHistoricalWorker.class);
    private final MemoryTracker memoryTracker;
    private final BasicMemoryBuffer<ConsumerRecord> buffer;

    protected KafkaConsumer<byte[], byte[]> consumer;

    private final StreamWorkerContext<ConsumerRecord> context;
    protected KafkaConfig config;

    private final MiddlewareBuffer middlewareBuffer;

    private final TargetConnectorCommitter committer;
    private final Queue<List<MiddlewareBuffer.TableCheckpoint>> checkpointQueue;
    private ExecutorService executor;
    private long sleepDurationInMillis;
    private long lastRunTimestamp;

    private Thread workerThread;
    private AtomicBoolean working;

    @Inject
    public KafkaHistoricalWorker(KafkaConfig config, MemoryTracker memoryTracker, MiddlewareConfig middlewareConfig, StreamWorkerContext<ConsumerRecord> context, TargetConnectorCommitter committer) {
        this.config = config;
        this.context = context;
        this.committer = committer;
        this.memoryTracker = memoryTracker;
        buffer = context.createBuffer();
        this.middlewareBuffer = new MiddlewareBuffer(middlewareConfig, memoryTracker);
        this.executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                .setNameFormat("kafka-topic-consumer").build());
        working = new AtomicBoolean(true);
        checkpointQueue = new ConcurrentLinkedQueue<>();
        sleepDurationInMillis = ChronoUnit.MINUTES.getDuration().multipliedBy(5).getSeconds();
    }

    @PreDestroy
    public void shutdown() {
        working.set(false);
    }

    @PostConstruct
    public void start() {
        workerThread = new Thread(this::execute);
        workerThread.start();
    }

    public void subscribe() {
        consumer = new KafkaConsumer(createConsumerConfig(config));
        consumer.subscribe(config.getTopic());
    }

    public void execute() {
        subscribe();

        try {
            while (working.get()) {
                long now = System.currentTimeMillis();
                if (now - lastRunTimestamp < sleepDurationInMillis) {
                    try {
                        SECONDS.sleep(100);
                    } catch (InterruptedException e) {
                        break;
                    }
                    continue;
                }

                ConsumerRecords<byte[], byte[]> records = consumer.poll(0);
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    if (KAFKA_KEY_FOR_HISTORICAL_DATA.equals(record.key())) {
                        buffer.consumePage(record, record.value().length);
                    } else {
                        buffer.consumeRecord(record, record.value().length);
                    }
                }

                if (buffer.shouldFlush()) {
                    BasicMemoryBuffer<ConsumerRecord>.Records recordList = buffer.getRecords();
                    buffer.clear();

                    Map<SchemaTableName, TableData> convert;
                    try {
                        convert = context.convert(recordList.buffer, recordList.bulkBuffer, recordList.pageBuffer);
                    } catch (IOException e) {
                        log.error(e, "Error processing Kafka records, passing record to latest offset.");
                        commitSyncOffset(consumer, null);
                        continue;
                    }

                    middlewareBuffer.add(new BatchRecords(convert, () -> commitSyncOffset(consumer, findLatestRecord(null))));

                    if (memoryTracker.availableMemory() == -1) {
                        try {
                            KafkaUtil.flushIfNeeded(middlewareBuffer, committer, checkpointQueue, memoryTracker, log);
                        } catch (Throwable e) {
                            log.error(e, "Error processing Kafka records, passing record to latest offset.");
                            commitSyncOffset(consumer, null);
                        }
                    }
                }

                List<MiddlewareBuffer.TableCheckpoint> poll = checkpointQueue.poll();
                while (poll != null) {
                    checkpoint(poll);
                    poll = checkpointQueue.poll();
                }
                lastRunTimestamp = now;
            }
        } finally {
            KafkaUtil.close(context, consumer, executor, log);
        }
    }


    public void checkpoint(List<MiddlewareBuffer.TableCheckpoint> value) {
        for (MiddlewareBuffer.TableCheckpoint tableCheckpoint : value) {
            try {
                tableCheckpoint.checkpoint();
            } catch (BatchRecords.CheckpointException e) {
                log.error(e, "Error while checkpointing records");
            }
        }
    }

    @SuppressWarnings("Duplicates")
    public void commitSyncOffset(KafkaConsumer<byte[], byte[]> consumer, ConsumerRecord record) {
        if (record == null) {
            consumer.commitSync();
        } else {
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
            consumer.commitSync(offsets);
        }
    }
}