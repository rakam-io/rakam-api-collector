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
import io.rakam.presto.kafka.KafkaJsonMessageTransformer;
import io.rakam.presto.kafka.KafkaRecordSizeCalculator;
import io.rakam.presto.kinesis.KinesisRecordProcessor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.util.AvroUtil;
import org.rakam.util.JsonHelper;

import javax.xml.validation.SchemaFactory;

import java.net.URI;
import java.time.LocalDate;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.rakam.presto.ServiceStarter.initializeLogging;
import static java.time.ZoneOffset.UTC;

public class KinesisStressTest
{
    private static final Logger log = Logger.get(KafkaStressTest.class);

    public static void main(String[] args)
            throws Exception
    {
        initializeLogging(System.getProperty("log.levels-file"));
//        final List<byte[]> consumerRecords = getDataForFabric();

        RaptorConfig raptorConfig = new RaptorConfig()
                .setMetadataUrl("jdbc:mysql://127.0.0.1/presto?user=root&password=&useSSL=false")
                .setPrestoURL(URI.create("http://127.0.0.1:8080"));

        FieldNameConfig fieldNameConfig = new FieldNameConfig();
        S3BackupConfig s3BackupConfig = new S3BackupConfig();
        s3BackupConfig.setS3Bucket("test");

        TestBackupStoreModule backupStoreModule = new TestBackupStoreModule((uuid, slice) -> {
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        MemoryTracker memoryTracker1 = new MemoryTracker(new MemoryTracker.MemoryConfig());

        RaptorDatabaseHandler databaseHandler = new RaptorDatabaseHandler(raptorConfig, new TypeRegistry(), s3BackupConfig, fieldNameConfig, memoryTracker1, backupStoreModule);

        AtomicLong committedRecords = new AtomicLong(0);

        StreamConfig streamConfig = new StreamConfig()
//                .setMaxFlushDuration(Duration.succinctNanos(0))
                ;
        MiddlewareConfig middlewareConfig = new MiddlewareConfig()
//                .setMaxFlushDuration(Duration.succinctNanos(0))
                ;
        CommitterConfig committerConfig = new CommitterConfig();
        JsonDeserializer deserializer = new FabricJsonDeserializer(databaseHandler, fieldNameConfig);
        KafkaJsonMessageTransformer transformer = new KafkaJsonMessageTransformer(fieldNameConfig, databaseHandler, deserializer);
        final MemoryTracker memoryTracker = memoryTracker1;

        StreamWorkerContext context = new StreamWorkerContext(transformer, memoryTracker1, new KafkaRecordSizeCalculator(), streamConfig);
        TargetConnectorCommitter targetConnectorCommitter = new TargetConnectorCommitter(databaseHandler, committerConfig);

        AtomicLong totalRecord = new AtomicLong(-1);
        AtomicLong lastPoll = new AtomicLong(System.currentTimeMillis());

        KinesisRecordProcessor processor = new KinesisRecordProcessor(context, new MiddlewareBuffer(middlewareConfig, memoryTracker), memoryTracker, targetConnectorCommitter);
        processor.initialize("test");

        while (true) {
            processor.processRecords(null, null);
        }
    }

    private static List<byte[]> getDataForRakam()
    {
        Random random = new Random();

//        return IntStream.range(0, 30000).mapToObj(i -> {
//            GenericData.Record record = new GenericData.Record(AVRO_SCHEMA);
//            return null;
//        }).collect(Collectors.toList());

        return IntStream.range(0, 30000).mapToObj(i -> JsonHelper.encodeAsBytes(ImmutableMap.of(
                "project", "demo_stress",
                "collection", "tweet" + (i % 100),
                "properties", ImmutableMap.builder()
                        .put("place", "USA")
                        .put("id", 34235435 * i)
                        .put("place_id", 45 * i)
                        .put("place_type", "tefdsfsdfts" + i)
                        .put("user_lang", "fdsfsdfen" + i)
                        .put("has_media", false)
                        .put("_time", i % 10 == 0 ? LocalDate.now().minusDays(random.nextInt(30)).atStartOfDay().toEpochSecond(UTC) * 1000 : (System.currentTimeMillis() - (i * 10000)))
                        .put("user_mentions", ImmutableList.of("test", "test3", "test" + i))
                        .put("is_retweet", true)
                        .put("country_code", "USA" + i)
                        .put("user_followers", 445 + i)
                        .put("language", "ENGLISH" + i)
                        .put("_device_id", "4353454534534534534trgd" + i)
                        .put("user_status_count", 3434 + i)
                        .put("user_created", 432342 + i)
                        .put("longitude", 432342 + i)
                        .put("is_reply", false)
                        .put("latitude", 432342 + i)
                        .put("is_positive", false).build()))).collect(Collectors.toList());

//        GenericData.Record record = new GenericData.Record(schema);
//        record.put("test", String.valueOf(i));

//        ByteArrayOutputStream out = new ByteArrayOutputStream();
//        BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(out, null);

//        GenericDatumWriter write = new GenericDatumWriter(record.getSchema());
//        write.write(record, binaryEncoder);
//
//        return out.toByteArray();

//        byte[] bytes = get(record);
    }

    private final static Schema AVRO_SCHEMA = AvroUtil.convertAvroSchema(ImmutableList.<SchemaField>builder()
            .add(new SchemaField("place", FieldType.STRING))
            .add(new SchemaField("id", FieldType.LONG))
            .add(new SchemaField("place_id", FieldType.LONG))
            .add(new SchemaField("place_type", FieldType.STRING))
            .add(new SchemaField("user_lang", FieldType.STRING))
            .add(new SchemaField("has_media", FieldType.BOOLEAN))
            .add(new SchemaField("_time", FieldType.TIMESTAMP))
            .add(new SchemaField("user_mentions", FieldType.ARRAY_STRING))
            .add(new SchemaField("is_retweet", FieldType.BOOLEAN))
            .add(new SchemaField("country_code", FieldType.STRING))
            .add(new SchemaField("user_followers", FieldType.LONG))
            .add(new SchemaField("language", FieldType.STRING))
            .add(new SchemaField("_device_id", FieldType.STRING))
            .add(new SchemaField("user_status_count", FieldType.LONG))
            .add(new SchemaField("user_created", FieldType.LONG))
            .add(new SchemaField("longitude", FieldType.LONG))
            .add(new SchemaField("is_reply", FieldType.BOOLEAN))
            .add(new SchemaField("latitude", FieldType.LONG))
            .add(new SchemaField("is_positive", FieldType.BOOLEAN))
            .build());
}
