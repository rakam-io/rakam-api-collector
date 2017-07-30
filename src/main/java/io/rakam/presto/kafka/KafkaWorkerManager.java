/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.rakam.presto.BasicMemoryBuffer;
import io.rakam.presto.BatchRecords;
import io.rakam.presto.MiddlewareBuffer;
import io.rakam.presto.MiddlewareConfig;
import io.rakam.presto.StreamWorkerContext;
import io.rakam.presto.TargetConnectorCommitter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.IOException;
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
    private final StreamWorkerContext<ConsumerRecord> context;
    private final TargetConnectorCommitter committer;
    private final MiddlewareBuffer middlewareBuffer;
    private final BasicMemoryBuffer buffer;
    private KafkaConsumer<byte[], byte[]> consumer;
    private KafkaConfig config;
    private ExecutorService executor;
    private ZooKeeper zk;

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
                System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        }
        catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        }
    }

    @PostConstruct
    public void run()
    {
        String zkNodes = config.getZookeeperNodes().stream().map(HostAddress::toString).collect(Collectors.joining(","));

        List<String> children;
        try {
            zk = new ZooKeeper(zkNodes, 1000 * 60 * 2, this);
            children = zk.getChildren("/brokers/topics", this);
        }
        catch (IOException | KeeperException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        consumer = new KafkaConsumer(createConsumerConfig(zkNodes, "127.0.0.1:9092"));
        consumer.subscribe(ImmutableList.of("presto.tweet"));

        try {

            while (true) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(1000);
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    buffer.consumeRecord(record, record.value().length);
                }
                if (buffer.shouldFlush()) {
                    Map.Entry<List, List> records1 = buffer.getRecords();
                    try {
                        middlewareBuffer.add(new BatchRecords(context.convert(records1.getKey(), records1.getValue()), new BatchRecords.Checkpointer() {
                            @Override
                            public void checkpoint()
                                    throws BatchRecords.CheckpointException
                            {
                                // checkpoint for kafka topic
                            }
                        }));

                        if(middlewareBuffer.shouldFlush()) {
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
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }

    private static Properties createConsumerConfig(String zkNodes, String kafkaNodes)
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