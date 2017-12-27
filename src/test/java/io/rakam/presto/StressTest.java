/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.rakam.presto.connector.raptor.RaptorConfig;
import io.rakam.presto.connector.raptor.RaptorDatabaseHandler;
import io.rakam.presto.connector.raptor.S3BackupConfig;
import io.rakam.presto.deserialization.json.FabricJsonDeserializer;
import io.rakam.presto.deserialization.json.JsonDeserializer;
import io.rakam.presto.deserialization.json.RakamJsonDeserializer;
import io.rakam.presto.kafka.KafkaConfig;
import io.rakam.presto.kafka.KafkaDecoupleMessage;
import io.rakam.presto.kafka.KafkaJsonMessageTransformer;
import io.rakam.presto.kafka.KafkaWorkerManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.rakam.util.JsonHelper;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.rakam.presto.kafka.KafkaConfig.DataFormat.JSON;

public class StressTest {
    private static final Logger log = Logger.get(StressTest.class);


    public static void main1(String[] args) throws IOException {
        TestDatabaseHandler databaseHandler = new TestDatabaseHandler();
        FieldNameConfig fieldNameConfig = new FieldNameConfig();
        KafkaJsonMessageTransformer messageTransformer = new KafkaJsonMessageTransformer(
                fieldNameConfig, databaseHandler, new FabricJsonDeserializer(databaseHandler, fieldNameConfig));

        List<byte[]> data = getDataForFabric();

        long l = System.currentTimeMillis();
        int i = 0;

        while (true) {
//            messageTransformer.createPageTable(data, ImmutableList.of());
            i += 30000;

            long gap = System.currentTimeMillis() - l;
            if (gap > 1000) {
                System.out.println(i + " records in " + gap + "ms");
                i = 0;
                l = System.currentTimeMillis();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        final List<byte[]> consumerRecords = getDataForFabric();

        KafkaConfig kafkaConfig = new KafkaConfig()
                .setDataFormat(JSON)
                .setNodes("127.0.0.1")
                .setZookeeperNodes("127.0.0.1")
                .setTopic("events");

        RaptorConfig raptorConfig = new RaptorConfig()
                .setMetadataUrl("jdbc:mysql://127.0.0.1/presto?user=root&password=")
                .setDataDirectory(new File("/tmp/test"))
                .setPrestoURL(URI.create("http://127.0.0.1:8080"));

        FieldNameConfig fieldNameConfig = new FieldNameConfig();
        S3BackupConfig s3BackupConfig = new S3BackupConfig();
        s3BackupConfig.setS3Bucket("test");

        RaptorDatabaseHandler databaseHandler = new RaptorDatabaseHandler(raptorConfig, s3BackupConfig, fieldNameConfig, new TestBackupStoreModule((uuid, file) -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));

        AtomicLong committedRecords = new AtomicLong(0);

        StreamConfig streamConfig = new StreamConfig()
//                .setMaxFlushRecords(0)
                ;
        MiddlewareConfig middlewareConfig = new MiddlewareConfig()
//                .setMaxFlushRecords(0)
                ;

        JsonDeserializer deserializer = new FabricJsonDeserializer(databaseHandler, fieldNameConfig);
        StreamWorkerContext context = new StreamWorkerContext(new KafkaJsonMessageTransformer(fieldNameConfig, databaseHandler, deserializer), streamConfig);
        TargetConnectorCommitter targetConnectorCommitter = new TargetConnectorCommitter(databaseHandler);


        AtomicLong totalRecord = new AtomicLong(-1);
        AtomicLong lastPoll = new AtomicLong(System.currentTimeMillis());

        KafkaWorkerManager kafkaWorkerManager = new KafkaWorkerManager(kafkaConfig, middlewareConfig,
                new KafkaDecoupleMessage(fieldNameConfig, streamConfig), context, targetConnectorCommitter) {
            @Override
            public void subscribe() {
                consumer = new KafkaConsumer(createConsumerConfig(config)) {
                    @Override
                    public ConsumerRecords poll(long timeout) {

                        long currentTotalRecord = totalRecord.get();
                        log.info("poll start. since last poll: " + ((System.currentTimeMillis() - lastPoll.get())) + "ms. total records:" + currentTotalRecord + ", lag: " + (currentTotalRecord - committedRecords.get()) + " records ");

                        try {
                            Thread.sleep(timeout);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        lastPoll.set(System.currentTimeMillis());

                        List<ConsumerRecord<byte[], byte[]>> records = IntStream.range(0, consumerRecords.size())
                                .mapToObj(i -> new ConsumerRecord<>("test", -1, currentTotalRecord + i, new byte[]{}, consumerRecords.get(i)))
                                .collect(Collectors.toList());

                        if (currentTotalRecord == -1) {
                            totalRecord.set(0);
                        } else {
                            totalRecord.addAndGet(consumerRecords.size());
                        }

                        return new ConsumerRecords(ImmutableMap.of(new TopicPartition("test", -1), records));
                    }
                };
            }

            @Override
            public void commitSyncOffset(ConsumerRecord record) {
                if (record == null) {
                    committedRecords.set(totalRecord.get());
                } else {
                    committedRecords.set(record.offset());
                }
            }
        };

        kafkaWorkerManager.run();
    }

    private static List<byte[]> getDataForFabric() {
        return IntStream.range(0, 30000).mapToObj(i -> JsonHelper.encodeAsBytes(ImmutableMap.of(
                "id", "test",
                "metadata", ImmutableMap.builder().build(),
                "data", ImmutableMap.builder()
                        .put("_project", "demo")
                        .put("_collection", "tweet3")
                        .put("place", "USA")
                        .put("id", 34235435 * i)
                        .put("place_id", 45 * i)
                        .put("place_type", "tefdsfsdfts" + i)
                        .put("user_lang", "fdsfsdfen" + i)
                        .put("has_media", false)
                        .put("_time", 5435435 + i)
                        .put("user_mentions", ImmutableList.of("test", "test3", "test" + i))
                        .put("is_retweet", true)
                        .put("country_code", "USA" + i)
                        .put("user_followers", 445 + i)
                        .put("language", "ENGLISH" + i)
                        .put("user_status_count", 3434 + i)
                        .put("_device_id", "4353454534534534534trgd" + i)
                        .put("user_created", 432342 + i)
                        .put("longitude", 432342 + i)
                        .put("is_reply", false)
                        .put("latitude", 432342 + i)
                        .put("is_positive", false).build()))).collect(Collectors.toList());
    }

    private static List<byte[]> getDataForRakam() {
        return IntStream.range(0, 30000).mapToObj(i -> JsonHelper.encodeAsBytes(ImmutableMap.of(
                "project", "demo",
                "collection", "tweet3",
                "properties", ImmutableMap.builder()
                        .put("place", "USA")
                        .put("id", 34235435 * i)
                        .put("place_id", 45 * i)
                        .put("place_type", "tefdsfsdfts" + i)
                        .put("user_lang", "fdsfsdfen" + i)
                        .put("has_media", false)
                        .put("_time", 5435435 + i)
                        .put("user_mentions", ImmutableList.of("test", "test3", "test" + i))
                        .put("is_retweet", true)
                        .put("country_code", "USA" + i)
                        .put("user_followers", 445 + i)
                        .put("language", "ENGLISH" + i)
                        .put("user_status_count", 3434 + i)
                        .put("_device_id", "4353454534534534534trgd" + i)
                        .put("user_created", 432342 + i)
                        .put("longitude", 432342 + i)
                        .put("is_reply", false)
                        .put("latitude", 432342 + i)
                        .put("is_positive", false).build()))).collect(Collectors.toList());
    }
}
