/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.log.Logger;
import io.rakam.presto.BasicMemoryBuffer;
import io.rakam.presto.BatchRecords;
import io.rakam.presto.MiddlewareBuffer;
import io.rakam.presto.MiddlewareBuffer.TableCheckpoint;
import io.rakam.presto.MiddlewareConfig;
import io.rakam.presto.StreamWorkerContext;
import io.rakam.presto.TargetConnectorCommitter;
import io.rakam.presto.deserialization.TableData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class KafkaWorkerManager
        implements Watcher {
    private static final Logger log = Logger.get(KafkaWorkerManager.class);

    private final StreamWorkerContext<ConsumerRecord> context;
    private final TargetConnectorCommitter committer;
    private final MiddlewareBuffer middlewareBuffer;
    private final BasicMemoryBuffer<ConsumerRecord> buffer;
    protected KafkaConsumer<byte[], byte[]> consumer;
    protected KafkaConfig config;
    private ExecutorService executor;
    private Thread workerThread;
    private AtomicBoolean working;

    @Inject
    public KafkaWorkerManager(KafkaConfig config, MiddlewareConfig middlewareConfig, StreamWorkerContext<ConsumerRecord> context, TargetConnectorCommitter committer) {
        this.config = config;
        this.context = context;
        this.committer = committer;
        this.middlewareBuffer = new MiddlewareBuffer(middlewareConfig);
        buffer = context.createBuffer();
        this.executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("kafka-topic-consumer").build());
        working = new AtomicBoolean(true);
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    @PreDestroy
    public void shutdown() {
        working.set(false);
    }

    @PostConstruct
    public void start() {
        workerThread = new Thread(this::run);
        workerThread.setDaemon(true);
        workerThread.start();
    }

    public void run() {
        consumer = new KafkaConsumer(createConsumerConfig(config));
        consumer.subscribe(config.getTopic());

        Queue<List<TableCheckpoint>> checkpintQueue = new ConcurrentLinkedQueue<>();

        try {
            while (working.get()) {
                ConsumerRecords<byte[], byte[]> kafkaRecord = consumer.poll(0);
                for (ConsumerRecord<byte[], byte[]> record : kafkaRecord) {
                    buffer.consumeRecord(record, record.value().length);
                }
                if (buffer.shouldFlush()) {
                    try {
                        Map.Entry<List<ConsumerRecord>, List<ConsumerRecord>> records = buffer.getRecords();

                        if (!records.getValue().isEmpty() || !records.getKey().isEmpty()) {
                            Map<SchemaTableName, TableData> convert = context.convert(records.getKey(), records.getValue());
                            buffer.clear();

                            middlewareBuffer.add(new BatchRecords(convert, () -> commitSyncOffset(findLatestRecord(records))));
                        }

                        Map<SchemaTableName, List<TableCheckpoint>> map = middlewareBuffer.flush();

                        if (!map.isEmpty()) {
                            for (Map.Entry<SchemaTableName, List<TableCheckpoint>> entry : map.entrySet()) {
                                committer.process(entry.getKey(), entry.getValue()).whenComplete((aVoid, throwable) -> {
                                    if (throwable != null) {
                                        log.error(throwable, "Error while processing records");
                                    }

                                    // TODO: What should we do if we can't process the data?
                                    checkpintQueue.add(entry.getValue());
                                });
                            }
                        }
                    } catch (Throwable e) {
                        log.error(e, "Error processing Kafka records, passing record to latest offset.");
                        commitSyncOffset(null);
                    }
                }

                List<TableCheckpoint> poll = checkpintQueue.poll();
                while (poll != null) {
                    checkpoint(poll);
                    poll = checkpintQueue.poll();
                }

            }
        } finally {
            context.shutdown();
            if (consumer != null) {
                consumer.close();
            }
            if (executor != null) {
                executor.shutdown();

                try {
                    if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                        log.warn("Timed out waiting for consumer threads to shut down, exiting uncleanly");
                    }
                } catch (InterruptedException e) {
                    log.warn("Interrupted during shutdown, exiting uncleanly");
                }
            }
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

    private Map<SchemaTableName, TableData> flushStream() {
        Map<SchemaTableName, TableData> pages;
        try {
            Map.Entry<List<ConsumerRecord>, List<ConsumerRecord>> list = buffer.getRecords();

            if (list.getValue().isEmpty() && list.getKey().isEmpty()) {
                return null;
            }

            pages = context.convert(list.getKey(), list.getValue());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        buffer.clear();
        return pages;
    }

    public void commitSyncOffset(ConsumerRecord record) {
        if (record == null) {
            consumer.commitSync();
        } else {
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
            consumer.commitSync(offsets);
        }
    }

    protected static Properties createConsumerConfig(KafkaConfig config) {
        String kafkaNodes = config.getNodes().stream().map(HostAddress::toString).collect(Collectors.joining(","));
        String offset = config.getOffset();
        String groupId = config.getGroupId();
        String sessionTimeOut = config.getSessionTimeOut();
        String requestTimeOut = config.getRequestTimeOut();

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaNodes);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", offset);
        props.put("session.timeout.ms", sessionTimeOut);
        props.put("heartbeat.interval.ms", "1000");
        props.put("request.timeout.ms", requestTimeOut);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        return props;
    }

    private ConsumerRecord findLatestRecord(Map.Entry<List<ConsumerRecord>, List<ConsumerRecord>> records) {
        ConsumerRecord lastSingleRecord = records.getKey().isEmpty() ? null : records.getKey().get(records.getKey().size() - 1);
        ConsumerRecord lastBatchRecord = records.getValue().isEmpty() ? null : records.getValue().get(records.getValue().size() - 1);

        ConsumerRecord lastRecord;
        if (lastBatchRecord != null && lastSingleRecord != null) {
            lastRecord = lastBatchRecord.offset() > lastBatchRecord.offset() ? lastBatchRecord : lastSingleRecord;
        } else {
            lastRecord = lastBatchRecord != null ? lastBatchRecord : lastSingleRecord;
        }

        return lastRecord;
    }

    @Override
    public void process(WatchedEvent event) {
        //
    }
}
