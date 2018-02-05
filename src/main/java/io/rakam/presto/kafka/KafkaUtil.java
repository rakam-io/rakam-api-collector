/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.SchemaTableName;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import io.airlift.units.Duration;
import io.rakam.presto.BasicMemoryBuffer;
import io.rakam.presto.MemoryTracker;
import io.rakam.presto.MiddlewareBuffer.TableCheckpoint;
import io.rakam.presto.StreamWorkerContext;
import io.rakam.presto.TargetConnectorCommitter;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.succinctDuration;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class KafkaUtil
{
    private static final Logger log = Logger.get(KafkaUtil.class);

    public static Properties createConsumerConfig(KafkaConfig config)
    {
        Properties props = createConfig(config);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        return props;
    }

    public static Properties createProducerConfig(KafkaConfig config, String transactionId)
    {
        Properties props = createConfig(config);
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        if (transactionId != null) {
            props.put("transactional.id", transactionId);
        }
        return props;
    }

    public static Properties createConfig(KafkaConfig config)
    {
        String kafkaNodes = config.getNodes().stream().map(HostAddress::toString).collect(Collectors.joining(","));
        String offset = config.getOffset();
        String groupId = config.getGroupId();
        String sessionTimeOut = config.getSessionTimeOut();
        String requestTimeOut = config.getRequestTimeOut();

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaNodes);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset);
        props.put("session.timeout.ms", sessionTimeOut);
        props.put("heartbeat.interval.ms", "1000");
        props.put("request.timeout.ms", requestTimeOut);
        props.put("max.poll.records", config.getMaxPollRecords());
        return props;
    }

    public static Map<String, Int2LongOpenHashMap> findLatestOffsets(Iterable<ConsumerRecord<byte[], byte[]>> streamRecords, Iterable<ConsumerRecord<byte[], byte[]>> bulkRecords)
    {
        HashMap<String, Int2LongOpenHashMap> topicMap = new HashMap<>();

        internalProcessRecords(topicMap, streamRecords);
        internalProcessRecords(topicMap, bulkRecords);

        return topicMap;
    }

    private static void internalProcessRecords(HashMap<String, Int2LongOpenHashMap> topicMap, Iterable<ConsumerRecord<byte[], byte[]>> records)
    {
        for (ConsumerRecord<byte[], byte[]> record : records) {
            Int2LongOpenHashMap map = topicMap.get(record.topic());
            if (map == null) {
                map = new Int2LongOpenHashMap();
                map.defaultReturnValue(-1);
                topicMap.put(record.topic(), map);
            }

            long value = map.get(record.partition());
            if (value == -1 || value < record.offset()) {
                map.put(record.partition(), record.offset());
            }
        }
    }

    public static void commitSyncOffset(KafkaConsumer<byte[], byte[]> consumer, Map<String, Int2LongOpenHashMap> offsets)
    {
        if (offsets == null) {
            consumer.commitSync();
        }
        else {
            Map<TopicPartition, OffsetAndMetadata> offsetsForKafka = new HashMap<>();
            for (Map.Entry<String, Int2LongOpenHashMap> entry : offsets.entrySet()) {
                String topic = entry.getKey();
                for (Int2LongMap.Entry offsetList : entry.getValue().int2LongEntrySet()) {
                    offsetsForKafka.put(new TopicPartition(topic, offsetList.getIntKey()), new OffsetAndMetadata(offsetList.getLongValue() + 1));
                }
            }

            consumer.commitSync(offsetsForKafka);
        }
    }

    public static void close(StreamWorkerContext context, KafkaConsumer consumer)
    {
        context.shutdown();
        if (consumer != null) {
            consumer.close();
        }
    }

    public static void flush(
            Map<SchemaTableName, List<TableCheckpoint>> map, TargetConnectorCommitter committer,
            Queue<List<TableCheckpoint>> checkpointQueue, MemoryTracker memoryTracker, Logger log,
            CounterStat databaseFlushStats, DistributionStat databaseFlushDistribution,
            CounterStat realTimeRecordsStats, CounterStat errorStats)
    {
        long totalRecords = map.entrySet().stream().mapToLong(e -> e.getValue().stream()
                .mapToLong(v -> v.getTable().page.getPositionCount()).sum()).sum();

        log.debug("Saving data, %d collections and %d events in total.", map.size(), totalRecords);


        for (Map.Entry<SchemaTableName, List<TableCheckpoint>> entry : map.entrySet()) {
            long now = System.currentTimeMillis();
            CompletableFuture<Void> dbWriteWork = committer.process(entry.getKey(), entry.getValue());
            dbWriteWork.whenComplete((aVoid, throwable) -> {
                long totalRecordCount = entry.getValue().stream()
                        .mapToLong(e -> e.getTable().page.getPositionCount())
                        .sum();

                long totalDataSize = entry.getValue().stream()
                        .mapToLong(e -> e.getTable().page.getRetainedSizeInBytes())
                        .sum();
                Duration totalDuration = succinctDuration(System.currentTimeMillis() - now, MILLISECONDS);
                if (throwable != null) {
                    errorStats.update(totalRecordCount);
                    log.error(throwable, "Error while processing records for collection %s", entry.getKey().toString());

                    double count = realTimeRecordsStats.getFiveMinute().getCount();
                    if (count > 100 && (errorStats.getFiveMinute().getCount() / count) > .4) {
                        log.error("The maximum error threshold is reached. Exiting the program...");
                        Runtime.getRuntime().exit(1);
                    }
                }
                else {
                    log.info("Saved data in buffer (%s - %d records) for collection %s in %s.",
                            succinctBytes(totalDataSize).toString(), totalRecordCount,
                            entry.getKey().toString(),
                            totalDuration.toString());
                }

                // TODO: What should we do if we can't commit the data?
                checkpointQueue.add(entry.getValue());

                databaseFlushStats.update(totalRecordCount);
                databaseFlushDistribution.add(totalDuration.toMillis());
                memoryTracker.freeMemory(totalDataSize);
            });
        }
    }
}
