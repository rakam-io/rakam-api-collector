/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.amazonaws.services.kinesis.model.Record;
import com.google.common.collect.ImmutableList;
import io.rakam.presto.deserialization.MessageEventTransformer;
import io.rakam.presto.deserialization.avro.AvroUtil;
import io.rakam.presto.kinesis.KinesisMessageEventTransformer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class TestKinesisAvroDeserializer
        extends TestDeserializer<Record>
{
    public MessageEventTransformer getMessageEventTransformer()
    {
        return new KinesisMessageEventTransformer(new FieldNameConfig(), new MemoryTracker(new MemoryTracker.MemoryConfig()), new TestDatabaseHandler("testproject", "testcollection", COLUMNS), new S3MiddlewareConfig());
    }

    public List<Record> getRecordsForEvents(String project, String collection, Optional<int[]> columnIdx)
            throws IOException
    {
        Schema schema = AvroUtil.convertAvroSchema(COLUMNS, "_shard_time");

        ImmutableList.Builder<Record> builder = ImmutableList.builder();
        for (Map<String, Object> event : EVENTS) {
            GenericData.Record record = new GenericData.Record(schema);

            for (Map.Entry<String, Object> entry : event.entrySet()) {

                if (!columnIdx.map(columIdxs -> Arrays.stream(columIdxs)
                        .mapToObj(i -> COLUMNS.get(i).getName()).anyMatch(e -> e.equals(entry.getKey()))).orElse(true)) {
                    continue;
                }

                record.put(entry.getKey(), convertAvroValue(entry.getValue()));
            }

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(2);
            BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(out, null);

            binaryEncoder.writeString(collection);
            GenericDatumWriter write = new GenericDatumWriter(record.getSchema());
            write.write(record, binaryEncoder);

            Record kinesisRecord = new Record();
            kinesisRecord.setData(ByteBuffer.wrap(out.toByteArray()));
            kinesisRecord.setPartitionKey(project + "|test");
            builder.add(kinesisRecord);
        }

        return builder.build();
    }

    private Object convertAvroValue(Object value)
    {
        if (value instanceof LocalDate) {
            value = (int) ((LocalDate) value).toEpochDay();
        }
        if (value instanceof ZonedDateTime) {
            value = ((ZonedDateTime) value).toEpochSecond() * 1000;
        }
        if (value instanceof LocalTime) {
            value = (long) ((LocalTime) value).getSecond();
        }
        if (value instanceof byte[]) {
            value = ByteBuffer.wrap((byte[]) value);
        }
        if (value instanceof Map) {
            Set<Map.Entry> set = ((Map) value).entrySet();
            Map<Object, Object> newMap = new HashMap<>();
            for (Map.Entry mapEntry : set) {
                newMap.put(mapEntry.getKey(), convertAvroValue(mapEntry.getValue()));
            }
            value = newMap;
        }
        if (value instanceof List) {
            List arrayValue = (List) value;
            List<Object> objects = new ArrayList<>();

            for (int i = 0; i < arrayValue.size(); i++) {
                objects.add(convertAvroValue(arrayValue.get(i)));
            }

            value = objects;
        }
        return value;
    }
}
