/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.rakam.presto.deserialization.json.FabricJsonDeserializer;
import io.rakam.presto.deserialization.json.JsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.rakam.util.JsonHelper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class TestKafkaFabricJsonDeserializer extends TestKafkaJsonDeserializer {
    @Override
    public JsonDeserializer getJsonDeserializer(DatabaseHandler databaseHandler) {
        return new FabricJsonDeserializer(databaseHandler, new FieldNameConfig());
    }

    @Override
    protected ImmutableList<ConsumerRecord<byte[], byte[]>> getSampleData() {
        ImmutableList.Builder<ConsumerRecord<byte[], byte[]>> builder = ImmutableList.builder();
        for (int i = 0; i < ITERATION_COUNT; i++) {
            ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test", -1, -1, new byte[] {}, JsonHelper.encodeAsBytes(ImmutableMap.of("data", ImmutableMap.of(
                    "_project", "testproject",
                    "_collection", "testcollection",
                    "newcolumn1", "test1",
                    "newcolumn2", "test2"))));
            builder.add(record);
        }
        return builder.build();
    }

    @Override
    protected ConsumerRecord<byte[], byte[]> getDuplicateFieldRecord() {
        byte[] data = "{\"data\": {\"_project\": \"testproject\", \"_collection\": \"testcollection\", \"testcolumn\": \"1\", \"testcolumn\": \"2\"}}".getBytes(StandardCharsets.UTF_8);
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test", -1, -1, new byte[]{}, data);
        return record;
    }

    @SuppressWarnings("Duplicates")
    public List<ConsumerRecord<byte[], byte[]>> getRecordsForEvents(String project, String collection, Optional<int[]> columnIdx)
            throws IOException {
        ImmutableList.Builder<ConsumerRecord<byte[], byte[]>> builder = ImmutableList.builder();
        for (Map<String, Object> event : EVENTS) {
            if (columnIdx.isPresent()) {
                event = new HashMap<>(event);

                for (Iterator<Map.Entry<String, Object>> it = event.entrySet().iterator(); it.hasNext(); ) {
                    Map.Entry<String, Object> entry = it.next();
                    if (!columnIdx.map(columIdxs -> Arrays.stream(columIdxs)
                            .mapToObj(i -> COLUMNS.get(i).getName()).anyMatch(e -> e.equals(entry.getKey()))).orElse(true)) {
                        it.remove();
                    }
                }
            }

            ImmutableMap.Builder<Object, Object> data = ImmutableMap.builder()
                    .put("_project", project)
                    .put("_collection", collection);

            event.forEach((s, o) -> data.put(s, o));

            ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test", -1, -1, new byte[]{}, JsonHelper.encodeAsBytes(ImmutableMap.of("data", data.build())));
            builder.add(record);
        }

        return builder.build();
    }
}