/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.block.Block;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;
import io.rakam.presto.deserialization.MessageEventTransformer;
import io.rakam.presto.deserialization.TableData;
import io.rakam.presto.deserialization.json.JsonDeserializer;
import io.rakam.presto.kafka.KafkaJsonMessageTransformer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.rakam.collection.FieldType;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public abstract class TestKafkaJsonDeserializer
        extends TestDeserializer<ConsumerRecord<byte[], byte[]>>
{
    public MessageEventTransformer getMessageEventTransformer()
    {
        TestDatabaseHandler databaseHandler = new TestDatabaseHandler("testproject", "testcollection", COLUMNS);
        return new KafkaJsonMessageTransformer(new FieldNameConfig(), databaseHandler, getJsonDeserializer(databaseHandler));
    }

    public abstract JsonDeserializer getJsonDeserializer(DatabaseHandler databaseHandler);
    protected abstract ImmutableList<ConsumerRecord<byte[],byte[]>> getSampleData();

    @Test
    public void testBasicAlterSchema()
            throws IOException
    {
        TestDatabaseHandler databaseHandler = new TestDatabaseHandler("testproject", "testcollection", COLUMNS, true);
        MessageEventTransformer messageEventTransformer = new KafkaJsonMessageTransformer(new FieldNameConfig(), databaseHandler, getJsonDeserializer(databaseHandler));

        ImmutableList<ConsumerRecord<byte[], byte[]>> build = getSampleData();

        Map<SchemaTableName, TableData> pageTable = messageEventTransformer.createPageTable(build, ImmutableList.of());
        TableData testcollection = pageTable.get(new SchemaTableName("testproject", "testcollection"));

        ImmutableList<ColumnMetadata> columns = ImmutableList.<ColumnMetadata>builder()
                .addAll(COLUMNS)
                .add(new ColumnMetadata("newcolumn1", VARCHAR))
                .add(new ColumnMetadata("newcolumn2", VARCHAR))
                .build();

        assertEquals(testcollection.metadata, columns);
        assertEquals(testcollection.page.getChannelCount(), columns.size());
        assertEquals(testcollection.page.getPositionCount(), ITERATION_COUNT);

        Block block1 = testcollection.page.getBlock(25);

        Block expectedBlock1 = BlockAssertions.createStringsBlock("test1", "test1", "test1");
        BlockAssertions.assertBlockEquals(VARCHAR, block1, expectedBlock1);

        Block block2 = testcollection.page.getBlock(26);

        Block expectedBlock2 = BlockAssertions.createStringsBlock("test2", "test2", "test2");
        BlockAssertions.assertBlockEquals(VARCHAR, block2, expectedBlock2);
    }

    @Test
    public void testDuplicateField()
            throws IOException
    {
        ConsumerRecord<byte[], byte[]> record = getDuplicateFieldRecord();
        TestDatabaseHandler databaseHandler = new TestDatabaseHandler();
        MessageEventTransformer messageEventTransformer = new KafkaJsonMessageTransformer(new FieldNameConfig(), databaseHandler, getJsonDeserializer(databaseHandler));
        Map<SchemaTableName, TableData> pageTable = messageEventTransformer.createPageTable(ImmutableList.of(record), ImmutableList.of());
        TableData testcollection = pageTable.get(new SchemaTableName("testproject", "testcollection"));

        assertEquals(testcollection.metadata.size(), 4);
        assertEquals(testcollection.page.getChannelCount(), 4);
        assertEquals(testcollection.page.getPositionCount(), 1);

        Block block = testcollection.page.getBlock(3);

        BlockAssertions.assertBlockEquals(COLUMNS.get(1).getType(), block,
                BlockAssertions.createStringsBlock("1"));
    }

    protected abstract ConsumerRecord<byte[],byte[]> getDuplicateFieldRecord();

    @Test
    public void testNewCollection()
            throws IOException
    {
        TestDatabaseHandler databaseHandler = new TestDatabaseHandler();
        MessageEventTransformer messageEventTransformer = new KafkaJsonMessageTransformer(new FieldNameConfig(), databaseHandler, getJsonDeserializer(databaseHandler));
        Map<SchemaTableName, TableData> pageTable = messageEventTransformer.createPageTable(getRecordsForEvents("testproject", "testcollection2", Optional.of(new int[] {
                1})), ImmutableList.of());
        TableData testcollection = pageTable.get(new SchemaTableName("testproject", "testcollection2"));

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
        MessageEventTransformer messageEventTransformer = new KafkaJsonMessageTransformer(new FieldNameConfig(), databaseHandler, getJsonDeserializer(databaseHandler));

        Map<SchemaTableName, TableData> pageTable = messageEventTransformer.createPageTable(getSampleData(), ImmutableList.of());
        TableData testcollection = pageTable.get(new SchemaTableName("testproject", "testcollection"));

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