/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import com.facebook.presto.spi.HostAddress;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.rakam.presto.StreamWorkerContext;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
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

public class KafkaWorkerManager
        implements Watcher
{
    private final StreamWorkerContext<MessageAndMetadata> context;
    private KafkaCommitter committer;
    private ConsumerConnector consumer;
    private KafkaConfig config;
    private ExecutorService executor;
    private ZooKeeper zk;
    private KafkaMemoryBuffer buffer;

    @Inject
    public KafkaWorkerManager(KafkaConfig config, StreamWorkerContext<MessageAndMetadata> context)
    {
        this.config = config;
        this.context = context;
        this.executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("kafka-topic-consumer").build());
        this.buffer = new KafkaMemoryBuffer();
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
            consumer.shutdown();
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
            throw Throwables.propagate(e);
        }

        this.consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zkNodes));
        this.committer = new KafkaCommitter(consumer, buffer, context);

        Map<String, Integer> collect = children.stream().collect(Collectors.toMap(c -> c, c -> 1));

        Map<String, List<KafkaStream<byte[], byte[]>>> consumers = consumer.createMessageStreams(collect);

        for (Map.Entry<String, List<KafkaStream<byte[], byte[]>>> entry : consumers.entrySet()) {
            String[] split = entry.getKey().split("_", 2);
//            executor.execute(new KafkaStreamConsumer(split[0], split[1], ImmutableSet.of(), entry.getValue().get(0), buffer, committer));
        }
//        Map<String, List<KafkaStream<byte[], byte[]>>> consumers = consumer.createMessageStreams(streamMap.build());
//
//        executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2,
//                new ThreadFactoryBuilder().setNameFormat("kafka-stream-worker").build());
//
//        for (Map.Entry<String, List<KafkaStream<byte[], byte[]>>> entry : consumers.entrySet()) {
//            for (KafkaStream<byte[], byte[]> stream : entry.getValue()) {
//                executor.execute(new KafkaStreamConsumer(entry.getKey(), consumer, stream, context));
//            }
//        }
    }

    private static ConsumerConfig createConsumerConfig(String zkNodes)
    {
        Properties props = new Properties();
        props.put("zookeeper.connect", zkNodes);
        props.put("group.id", "presto_streaming");
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.enable", "false");
        props.put("auto.offset.reset", "smallest");
        props.put("offsets.storage", "kafka");
        props.put("consumer.timeout.ms", "10");

        return new ConsumerConfig(props);
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