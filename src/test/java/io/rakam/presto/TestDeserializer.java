/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;
import io.rakam.presto.deserialization.MessageEventTransformer;
import io.rakam.presto.deserialization.TableData;
import org.rakam.collection.FieldType;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class TestDeserializer<T>
{
    protected static final List<FieldType> FIELDS = Arrays.stream(FieldType.values())
            .filter(e -> e != FieldType.DECIMAL && e != FieldType.ARRAY_DECIMAL && e != FieldType.MAP_DECIMAL)
            .filter(e -> e != FieldType.BINARY && e != FieldType.ARRAY_BINARY && e != FieldType.MAP_BINARY)
            .collect(toImmutableList());

    protected static final List<ColumnMetadata> COLUMNS;
    static {
        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();
        builder.add(new ColumnMetadata("_shard_time", TIMESTAMP));
        FIELDS.stream()
                .forEach(e -> builder.add(new ColumnMetadata("col" + e.name().toLowerCase(), toType(e))));
        COLUMNS = builder.build();
    }

    protected static final int ITERATION_COUNT = 3;
    protected static final List<Map<String, Object>> EVENTS;

    static {
        List<Map<String, Object>> events = new ArrayList<>();

        for (int i = 0; i < ITERATION_COUNT; i++) {
            HashMap<String, Object> event = new HashMap<>();

            for (int i1 = 0; i1 < FIELDS.size(); i1++) {
                FieldType value = FIELDS.get(i1);
                event.put("col" + value.name().toLowerCase(), getExampleValue(value, i));
            }

            events.add(Collections.unmodifiableMap(event));
        }

        EVENTS = ImmutableList.copyOf(events);
    }

    private static Object getExampleValue(FieldType fieldType, int i)
    {
        switch (fieldType) {
            case STRING:
                return String.valueOf(i);
            case INTEGER:
                return i;
            case DOUBLE:
                return (double) i;
            case LONG:
                return (long) i;
            case BOOLEAN:
                return i % 2 == 0;
            case DATE:
                return java.time.LocalDate.ofEpochDay(i);
            case TIME:
                return java.time.LocalTime.ofSecondOfDay(i);
            case TIMESTAMP:
                return ZonedDateTime.ofInstant(Instant.ofEpochSecond(i), ZoneOffset.UTC);
            case BINARY:
                return new byte[] {(byte) i};
            default:
                if (fieldType.isArray()) {
                    List<Object> objects = new ArrayList<>();
                    for (int i1 = 0; i1 < ITERATION_COUNT; i1++) {
                        objects.add(getExampleValue(fieldType.getArrayElementType(), i1));
                    }
                    return Collections.unmodifiableList(objects);
                }
                else if (fieldType.isMap()) {
                    Map<String, Object> map = new HashMap<>();

                    for (int i1 = 0; i1 < ITERATION_COUNT; i1++) {
                        map.put(String.valueOf(i1), getExampleValue(fieldType.getMapValueType(), i1));
                    }

                    return Collections.unmodifiableMap(map);
                }
                throw new IllegalStateException();
        }
    }

    public abstract MessageEventTransformer getMessageEventTransformer();

    public abstract List<T> getRecordsForEvents(String project, String collection, Optional<int[]> columnIdx)
            throws IOException;

    @Test
    public void testAllTypes()
            throws IOException
    {
        MessageEventTransformer messageEventTransformer = getMessageEventTransformer();
        Map<SchemaTableName, TableData> pageTable = messageEventTransformer.createPageTable(getRecordsForEvents("testproject", "testcollection", Optional.empty()), ImmutableList.of());
        TableData testcollection = pageTable.get(new SchemaTableName("testproject", "testcollection"));

        assertEquals(testcollection.metadata, COLUMNS);
        assertEquals(testcollection.page.getChannelCount(), COLUMNS.size());
        assertEquals(testcollection.page.getPositionCount(), ITERATION_COUNT);

        long aLong = BigintType.BIGINT.getLong(testcollection.page.getBlock(0), 0);
        assertTrue(Instant.now().toEpochMilli() - aLong < 10000);
        Block expectedShardTime = RunLengthEncodedBlock.create(TIMESTAMP, aLong, testcollection.page.getPositionCount());
        BlockAssertions.assertBlockEquals(TIMESTAMP, testcollection.page.getBlock(0), expectedShardTime);

        for (int i = 1; i < testcollection.page.getBlocks().length; i++) {
            Block block = testcollection.page.getBlock(i);

            FieldType fieldType = FIELDS.get(i - 1);
            Block expectedBlock = getBlock(fieldType);

            BlockAssertions.assertBlockEquals(COLUMNS.get(i).getType(), block, expectedBlock);
        }
    }

    @Test
    public void testNullValues()
            throws IOException
    {
        MessageEventTransformer messageEventTransformer = getMessageEventTransformer();

        Map<SchemaTableName, TableData> pageTable = messageEventTransformer.createPageTable(getRecordsForEvents("testproject", "testcollection", Optional.of(new int[] {
                1})), ImmutableList.of());
        TableData testcollection = pageTable.get(new SchemaTableName("testproject", "testcollection"));

        assertEquals(testcollection.metadata, COLUMNS);
        assertEquals(testcollection.page.getChannelCount(), COLUMNS.size());
        assertEquals(testcollection.page.getPositionCount(), ITERATION_COUNT);

        Block block = testcollection.page.getBlock(1);
        Block expectedBlock = getBlock(FieldType.STRING);
        BlockAssertions.assertBlockEquals(VARCHAR, block, expectedBlock);

        for (int i = 2; i < COLUMNS.size(); i++) {
            Block block1 = testcollection.page.getBlock(i);
            assertEquals(block1.getPositionCount(), ITERATION_COUNT);

            for (int i1 = 0; i1 < ITERATION_COUNT; i1++) {
                assertTrue(block1.isNull(i1));
            }
        }
    }

    public static Block getBlock(FieldType fieldType)
    {
        switch (fieldType) {
            case STRING:
                return BlockAssertions.createStringsBlock("0", "1", "2");
            case INTEGER:
                return BlockAssertions.createIntsBlock(0, 1, 2);
            case DOUBLE:
                return BlockAssertions.createDoublesBlock(0.0, 1.0, 2.0);
            case LONG:
                return BlockAssertions.createLongsBlock(0, 1, 2);
            case BOOLEAN:
                return BlockAssertions.createBooleansBlock(0 % 2 == 0, 1 % 2 == 0, 2 % 2 == 0);
            case DATE:
                return BlockAssertions.createDateSequenceBlock(0, 3);
            case TIMESTAMP:
                BlockBuilder blockBuilder = TIMESTAMP.createFixedSizeBlockBuilder(3);

                for (int i = 0; i < 3; i++) {
                    TIMESTAMP.writeLong(blockBuilder, i * 1000);
                }

                return blockBuilder.build();
            case TIME:
                return BlockAssertions.createTimestampSequenceBlock(0, 3);
            default:
                if (fieldType.isArray()) {
                    ArrayType arrayType = new ArrayType(toType(fieldType.getArrayElementType()));
                    BlockBuilder builder = arrayType.createBlockBuilder(new BlockBuilderStatus(), 100);

                    for (int i = 0; i < ITERATION_COUNT; i++) {
                        arrayType.writeObject(builder, getBlock(fieldType.getArrayElementType()));
                    }

                    return builder.build();
                }

                if (fieldType.isMap()) {
                    MapType mapType = new MapType(false, VARCHAR, toType(fieldType.getMapValueType()), null, null, null);
                    BlockBuilder builder = mapType.createBlockBuilder(new BlockBuilderStatus(), 100);

                    Block key = getBlock(FieldType.STRING);
                    Block value = getBlock(fieldType.getMapValueType());

                    for (int itemIdx = 0; itemIdx < ITERATION_COUNT; itemIdx++) {
                        BlockBuilder singleBlock = builder.beginBlockEntry();

                        for (int i = 0; i < ITERATION_COUNT; i++) {
                            key.writePositionTo(i, singleBlock);
                            singleBlock.closeEntry();
                            value.writePositionTo(i, singleBlock);
                            singleBlock.closeEntry();
                        }

                        builder.closeEntry();
                    }

                    return builder.build();
                }

                throw new IllegalStateException();
        }
    }

    public static Type toType(FieldType type)
    {
        switch (type) {
            case DOUBLE:
                return DoubleType.DOUBLE;
            case LONG:
                return BigintType.BIGINT;
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case STRING:
                return VARCHAR;
            case INTEGER:
                return IntegerType.INTEGER;
            case DATE:
                return DateType.DATE;
            case TIMESTAMP:
                return TimestampType.TIMESTAMP;
            case TIME:
                return TimeType.TIME;
            case BINARY:
                return VarbinaryType.VARBINARY;
            default:
                if (type.isArray()) {
                    return new ArrayType(toType(type.getArrayElementType()));
                }
                if (type.isMap()) {
                    return new MapType(false, VARCHAR, toType(type.getMapValueType()), null, null, null);
                }
                throw new IllegalStateException();
        }
    }
}
