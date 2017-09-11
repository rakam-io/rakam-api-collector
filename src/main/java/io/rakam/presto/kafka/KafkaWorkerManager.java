/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.log.Logger;
import io.rakam.presto.BasicMemoryBuffer;
import io.rakam.presto.BatchRecords;
import io.rakam.presto.MiddlewareBuffer;
import io.rakam.presto.MiddlewareConfig;
import io.rakam.presto.StreamWorkerContext;
import io.rakam.presto.TargetConnectorCommitter;
import io.rakam.presto.deserialization.TableData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@SuppressWarnings("ALL")
public class KafkaWorkerManager
        implements Watcher
{
    private static final Logger log = Logger.get(KafkaWorkerManager.class);

    private final StreamWorkerContext<ConsumerRecord> context;
    private final TargetConnectorCommitter committer;
    private final MiddlewareBuffer middlewareBuffer;
    private final BasicMemoryBuffer<ConsumerRecord> buffer;
    protected KafkaConsumer<byte[], byte[]> consumer;
    protected KafkaConfig config;
    private ExecutorService executor;

    @Inject
    public KafkaWorkerManager(KafkaConfig config, MiddlewareConfig middlewareConfig, StreamWorkerContext<ConsumerRecord> context, TargetConnectorCommitter committer)
    {
        this.config = config;
        this.context = context;
        this.committer = committer;
        this.middlewareBuffer = new MiddlewareBuffer(middlewareConfig);
        buffer = context.createBuffer();
        this.executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("kafka-topic-consumer").build());
    }

    public ExecutorService getExecutor()
    {
        return executor;
    }

    @PreDestroy
    public void shutdown()
    {
        context.shutdown();
        if (consumer != null) {
            consumer.close();
        }
        if (executor != null) {
            executor.shutdown();
        }
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                log.warn("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        }
        catch (InterruptedException e) {
            log.warn("Interrupted during shutdown, exiting uncleanly");
        }
    }

    public void subscribe() {
        String zkNodes = config.getZookeeperNodes().stream().map(HostAddress::toString).collect(Collectors.joining(","));
        String kafkaNodes = config.getNodes().stream().map(HostAddress::toString).collect(Collectors.joining(","));

        consumer = new KafkaConsumer(createConsumerConfig(zkNodes, kafkaNodes));
        consumer.subscribe(ImmutableList.of(config.getTopic()));
    }

    @PostConstruct
    public void run()
    {
        subscribe();

        try {
            while (true) {
                ConsumerRecords<byte[], byte[]> kafkaRecord = consumer.poll(0);
                for (ConsumerRecord<byte[], byte[]> record : kafkaRecord) {
                    buffer.consumeRecord(record, record.value().length);
                }
                if (buffer.shouldFlush()) {
                    try {
                        Map.Entry<List<ConsumerRecord>, List<ConsumerRecord>> records = buffer.getRecords();

                        if (!records.getValue().isEmpty() || !records.getKey().isEmpty()) {
                            Table<String, String, TableData> convert = context.convert(records.getKey(), records.getValue());
                            buffer.clear();

                            middlewareBuffer.add(new BatchRecords(convert, createCheckpointer(findLatestRecord(records))));
                        }

                        if (middlewareBuffer.shouldFlush()) {
                            List<BatchRecords> list = middlewareBuffer.flush();

                            if (!list.isEmpty()) {
                                committer.process(Iterables.transform(list, BatchRecords::getTable));

                                list.forEach(l -> {
                                    try {
                                        l.checkpoint();
                                    }
                                    catch (BatchRecords.CheckpointException e) {
                                        throw new RuntimeException("Error while checkpointing records", e);
                                    }
                                });
                            }
                        }
                    }
                    catch (Throwable e) {
                        log.error(e, "Error processing Kafka records, passing record to latest offset.");
                        commitAsyncOffset(null);
                    }
                }
            }
        }
        finally {
            consumer.close();
        }
    }

    private Table<String, String, TableData> flushStream()
    {
        Table<String, String, TableData> pages;
        try {
            Map.Entry<List<ConsumerRecord>, List<ConsumerRecord>> list = buffer.getRecords();

            if (list.getValue().isEmpty() && list.getKey().isEmpty()) {
                return null;
            }

            pages = context.convert(list.getKey(), list.getValue());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        buffer.clear();
        return pages;
    }

    private BatchRecords.Checkpointer createCheckpointer(ConsumerRecord record)
    {
        return new BatchRecords.Checkpointer()
        {
            @Override
            public void checkpoint()
                    throws BatchRecords.CheckpointException
            {
                commitAsyncOffset(record);
            }
        };
    }

    public void commitAsyncOffset(ConsumerRecord record)
    {
        OffsetCommitCallback callback = new OffsetCommitCallback()
        {
            @Override
            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e)
            {
                if (e != null) {
                    log.error(e, "Kafka offset commit fail. %s", offsets.toString());
                }
            }
        };

        if (record == null) {
            consumer.commitAsync(callback);
        }
        else {
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
            consumer.commitAsync(offsets, callback);
        }
    }

    protected static Properties createConsumerConfig(String zkNodes, String kafkaNodes)
    {
        Properties props = new Properties();
        props.put("zookeeper.connect", zkNodes);
        props.put("bootstrap.servers", kafkaNodes);
        props.put("group.id", "presto_streaming");
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.enable", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("offsets.storage", "kafka");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("consumer.timeout.ms", "10");

        return props;
    }

    private ConsumerRecord findLatestRecord(Map.Entry<List<ConsumerRecord>, List<ConsumerRecord>> records)
    {
        ConsumerRecord lastSingleRecord = records.getKey().isEmpty() ? null : records.getKey().get(records.getKey().size() - 1);
        ConsumerRecord lastBatchRecord = records.getValue().isEmpty() ? null : records.getValue().get(records.getValue().size() - 1);

        ConsumerRecord lastRecord;
        if (lastBatchRecord != null && lastSingleRecord != null) {
            lastRecord = lastBatchRecord.offset() > lastBatchRecord.offset() ? lastBatchRecord : lastSingleRecord;
        }
        else {
            lastRecord = lastBatchRecord != null ? lastBatchRecord : lastSingleRecord;
        }

        return lastRecord;
    }

    @Override
    public void process(WatchedEvent event)
    {
        switch (event.getType()) {
            case NodeChildrenChanged:
                break;
        }
        event.getPath();
    }
}