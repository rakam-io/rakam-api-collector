/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.block.Block;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Table;
import io.rakam.presto.deserialization.MessageEventTransformer;
import io.rakam.presto.deserialization.TableData;
import io.rakam.presto.kafka.KafkaJsonMessageTransformer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.rakam.collection.FieldType;
import org.rakam.util.JsonHelper;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestKafkaJsonDeserializer
        extends TestDeserializer<ConsumerRecord<byte[], byte[]>>
{
    public MessageEventTransformer getMessageEventTransformer()
    {
        return new KafkaJsonMessageTransformer(new FieldNameConfig(), new TestDatabaseHandler("testproject", "testcollection", COLUMNS));
    }

    public List<ConsumerRecord<byte[], byte[]>> getRecords(String project, String collection, Optional<int[]> columnIdx)
            throws IOException
    {
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
            ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test", -1, -1, new byte[] {}, JsonHelper.encodeAsBytes(ImmutableMap.of(
                    "project", project,
                    "collection", collection,
                    "properties", event)));
            builder.add(record);
        }

        return builder.build();
    }

    @Test
    public void testBasicAlterSchema()
            throws IOException
    {
        MessageEventTransformer messageEventTransformer = new KafkaJsonMessageTransformer(new FieldNameConfig(), new TestDatabaseHandler("testproject", "testcollection", COLUMNS, true));

        ImmutableList.Builder<ConsumerRecord<byte[], byte[]>> builder = ImmutableList.builder();
        for (int i = 0; i < ITERATION_COUNT; i++) {
            ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test", -1, -1, new byte[] {}, JsonHelper.encodeAsBytes(ImmutableMap.of(
                    "project", "testproject",
                    "collection", "testcollection",
                    "properties", ImmutableMap.of("newcolumn", "test"))));
            builder.add(record);
        }

        Table<String, String, TableData> pageTable = messageEventTransformer.createPageTable(builder.build(), ImmutableList.of());
        Map<String, TableData> testproject = pageTable.row("testproject");
        TableData testcollection = testproject.get("testcollection");

        ImmutableList<ColumnMetadata> columns = ImmutableList.<ColumnMetadata>builder()
                .addAll(COLUMNS).add(new ColumnMetadata("newcolumn", VARCHAR)).build();

        assertEquals(testcollection.metadata, columns);
        assertEquals(testcollection.page.getChannelCount(), columns.size());
        assertEquals(testcollection.page.getPositionCount(), ITERATION_COUNT);

        Block block = testcollection.page.getBlock(25);
        Block expectedBlock = BlockAssertions.createStringsBlock("test", "test", "test");
        BlockAssertions.assertBlockEquals(VARCHAR, block, expectedBlock);
    }

    @Test
    public void testDuplicateField()
            throws IOException
    {
        String data = "{\"project\": \"testproject\", " +
                "\"collection\": \"testcollection\", \"properties\": {\"testcolumn\": \"1\", \"testcolumn\": \"2\"}}";
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test", -1, -1, new byte[] {}, data.getBytes(StandardCharsets.UTF_8));

        TestDatabaseHandler databaseHandler = new TestDatabaseHandler();
        MessageEventTransformer messageEventTransformer = new KafkaJsonMessageTransformer(new FieldNameConfig(), databaseHandler);
        Table<String, String, TableData> pageTable = messageEventTransformer.createPageTable(ImmutableList.of(record), ImmutableList.of());
        Map<String, TableData> testproject = pageTable.row("testproject");
        TableData testcollection = testproject.get("testcollection");
        assertEquals(testcollection.metadata.size(), 4);
        assertEquals(testcollection.page.getChannelCount(), 4);
        assertEquals(testcollection.page.getPositionCount(), 1);

        Block block = testcollection.page.getBlock(3);

        BlockAssertions.assertBlockEquals(COLUMNS.get(1).getType(), block,
                BlockAssertions.createStringsBlock("1"));
    }

    @Test
    public void testNewCollection()
            throws IOException
    {
        TestDatabaseHandler databaseHandler = new TestDatabaseHandler();
        MessageEventTransformer messageEventTransformer = new KafkaJsonMessageTransformer(new FieldNameConfig(), databaseHandler);
        Table<String, String, TableData> pageTable = messageEventTransformer.createPageTable(getRecords("testproject", "testcollection2", Optional.of(new int[] {
                1})), ImmutableList.of());
        Map<String, TableData> testproject = pageTable.row("testproject");
        TableData testcollection = testproject.get("testcollection2");
        assertEquals(testcollection.metadata.size(), 4);
        assertEquals(testcollection.page.getChannelCount(), 4);
        assertEquals(testcollection.page.getPositionCount(), ITERATION_COUNT);

        Block block = testcollection.page.getBlock(3);

        // first field
        BlockAssertions.assertBlockEquals(VARCHAR, block, getBlock(FieldType.STRING));
    }

    @Test
    public void testComplexAlterSchema()
            throws IOException
    {
        ColumnMetadata newcolumn2 = new ColumnMetadata("newcolumn2", VARCHAR);
        ColumnMetadata newcolumn1 = new ColumnMetadata("newcolumn1", VARCHAR);
        ImmutableList<ColumnMetadata> latestColumns = ImmutableList.<ColumnMetadata>builder()
                .addAll(COLUMNS).add(newcolumn2).add(newcolumn1).build();

        TestDatabaseHandler databaseHandler = new TestDatabaseHandler("testproject", "testcollection", COLUMNS, true)
        {
            @Override
            public synchronized List<ColumnMetadata> addColumns(String schema, String table, List<ColumnMetadata> newColumns)
            {
                // Change the ordering
                assertEquals(newColumns, ImmutableList.of(newcolumn1, newcolumn2));
                return latestColumns;
            }
        };
        MessageEventTransformer messageEventTransformer = new KafkaJsonMessageTransformer(new FieldNameConfig(), databaseHandler);

        ImmutableList.Builder<ConsumerRecord<byte[], byte[]>> builder = ImmutableList.builder();
        for (int i = 0; i < ITERATION_COUNT; i++) {
            ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test", -1, -1, new byte[] {}, JsonHelper.encodeAsBytes(ImmutableMap.of(
                    "project", "testproject",
                    "collection", "testcollection",
                    "properties", ImmutableMap.of("newcolumn1", "test1", "newcolumn2", "test2"))));
            builder.add(record);
        }

        Table<String, String, TableData> pageTable = messageEventTransformer.createPageTable(builder.build(), ImmutableList.of());
        Map<String, TableData> testproject = pageTable.row("testproject");
        TableData testcollection = testproject.get("testcollection");

        assertEquals(testcollection.metadata, latestColumns);
        assertEquals(testcollection.page.getChannelCount(), latestColumns.size());
        assertEquals(testcollection.page.getPositionCount(), ITERATION_COUNT);

        Block block = testcollection.page.getBlock(25);
        Block expectedBlock = BlockAssertions.createStringsBlock("test2", "test2", "test2");
        BlockAssertions.assertBlockEquals(VARCHAR, block, expectedBlock);

        block = testcollection.page.getBlock(26);
        expectedBlock = BlockAssertions.createStringsBlock("test1", "test1", "test1");
        BlockAssertions.assertBlockEquals(VARCHAR, block, expectedBlock);
    }
}