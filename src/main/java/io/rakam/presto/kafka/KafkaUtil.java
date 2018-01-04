/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.SchemaTableName;
import io.airlift.log.Logger;
import io.rakam.presto.*;
import io.rakam.presto.MiddlewareBuffer.TableCheckpoint;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class KafkaUtil
{

    public static Properties createConsumerConfig(KafkaConfig config)
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
        props.put("auto.offset.reset", offset);
        props.put("session.timeout.ms", sessionTimeOut);
        props.put("heartbeat.interval.ms", "1000");
        props.put("request.timeout.ms", requestTimeOut);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        return props;
    }

    public static ConsumerRecord findLatestRecord(BasicMemoryBuffer<ConsumerRecord<byte[], byte[]>>.Records records)
    {
        ConsumerRecord lastSingleRecord = records.buffer.isEmpty() ? null : records.buffer.get(records.buffer.size() - 1);
        ConsumerRecord lastBatchRecord = records.bulkBuffer.isEmpty() ? null : records.bulkBuffer.get(records.bulkBuffer.size() - 1);

        ConsumerRecord lastRecord;
        if (lastBatchRecord != null && lastSingleRecord != null) {
            lastRecord = lastBatchRecord.offset() > lastBatchRecord.offset() ? lastBatchRecord : lastSingleRecord;
        }
        else {
            lastRecord = lastBatchRecord != null ? lastBatchRecord : lastSingleRecord;
        }

        return lastRecord;
    }

    public static void close(StreamWorkerContext context, KafkaConsumer consumer, ExecutorService executor, Logger log)
    {
        context.shutdown();
        if (consumer != null) {
            consumer.close();
        }
        if (executor != null) {
            executor.shutdown();

            try {
                if (!executor.awaitTermination(5000, MILLISECONDS)) {
                    log.warn("Timed out waiting for consumer threads to shut down, exiting uncleanly");
                }
            }
            catch (InterruptedException e) {
                log.warn("Interrupted during shutdown, exiting uncleanly");
            }
        }
    }

    public static void flushIfNeeded(MiddlewareBuffer middlewareBuffer, TargetConnectorCommitter committer, Queue<List<TableCheckpoint>> checkpointQueue, MemoryTracker memoryTracker, Logger log)
    {
        Map<SchemaTableName, List<TableCheckpoint>> map = middlewareBuffer.getRecordsToBeFlushed();

        if (!map.isEmpty()) {
            for (Map.Entry<SchemaTableName, List<TableCheckpoint>> entry : map.entrySet()) {
                CompletableFuture<Void> dbWriteWork = committer.process(entry.getKey(), entry.getValue());
                dbWriteWork.whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        log.error(throwable, "Error while processing records");
                    }

                    // TODO: What should we do if we can't commit the data?
                    checkpointQueue.add(entry.getValue());

                    long totalDataSize = entry.getValue().stream()
                            .mapToLong(e -> e.getTable().page.getRetainedSizeInBytes())
                            .sum();
                    memoryTracker.freeMemory(totalDataSize);
                });
            }
        }
    }
}
