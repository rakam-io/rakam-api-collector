/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public final class BlockAssertions
{
    private BlockAssertions()
    {
    }

    public static Object getOnlyValue(Type type, Block block)
    {
        assertEquals(block.getPositionCount(), 1, "Block positions");
        return type.getObjectValue(SESSION, block, 0);
    }

    public static List<Object> toValues(Type type, Iterable<Block> blocks)
    {
        List<Object> values = new ArrayList<>();
        for (Block block : blocks) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                values.add(type.getObjectValue(SESSION, block, position));
            }
        }
        return Collections.unmodifiableList(values);
    }

    public static List<Object> toValues(Type type, Block block)
    {
        List<Object> values = new ArrayList<>();
        for (int position = 0; position < block.getPositionCount(); position++) {
            values.add(type.getObjectValue(SESSION, block, position));
        }
        return Collections.unmodifiableList(values);
    }

    public static void assertBlockEquals(Type type, Block actual, Block expected)
    {
        for (int position = 0; position < actual.getPositionCount(); position++) {
            assertEquals(type.getObjectValue(SESSION, actual, position), type.getObjectValue(SESSION, expected, position));
        }
    }

    public static Block createStringsBlock(String... values)
    {
        requireNonNull(values, "varargs 'values' is null");

        return createStringsBlock(Arrays.asList(values));
    }

    public static Block createStringsBlock(Iterable<String> values)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 100);

        for (String value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                VARCHAR.writeString(builder, value);
            }
        }

        return builder.build();
    }

    public static Block createStringSequenceBlock(int start, int end)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 100);

        for (int i = start; i < end; i++) {
            VARCHAR.writeString(builder, String.valueOf(i));
        }

        return builder.build();
    }

    public static Block createStringArraysBlock(Iterable<? extends Iterable<String>> values)
    {
        ArrayType arrayType = new ArrayType(VARCHAR);
        BlockBuilder builder = arrayType.createBlockBuilder(new BlockBuilderStatus(), 100);

        for (Iterable<String> value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                arrayType.writeObject(builder, createStringsBlock(value));
            }
        }

        return builder.build();
    }

    public static Block createBooleansBlock(Boolean... values)
    {
        requireNonNull(values, "varargs 'values' is null");

        return createBooleansBlock(Arrays.asList(values));
    }

    public static Block createBooleansBlock(Boolean value, int count)
    {
        return createBooleansBlock(Collections.nCopies(count, value));
    }

    public static Block createBooleansBlock(Iterable<Boolean> values)
    {
        BlockBuilder builder = BOOLEAN.createBlockBuilder(new BlockBuilderStatus(), 100);

        for (Boolean value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                BOOLEAN.writeBoolean(builder, value);
            }
        }

        return builder.build();
    }

    public static Block createEmptyLongsBlock()
    {
        return BIGINT.createFixedSizeBlockBuilder(0).build();
    }

    public static Block createLongsBlock(int... values)
    {
        BlockBuilder builder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), 100);

        for (int value : values) {
            BIGINT.writeLong(builder, (long) value);
        }

        return builder.build();
    }

    public static Block createIntsBlock(int... values)
    {
        BlockBuilder builder = INTEGER.createBlockBuilder(new BlockBuilderStatus(), 100);

        for (int value : values) {
            INTEGER.writeLong(builder, value);
        }

        return builder.build();
    }

    public static Block createLongsBlock(Long... values)
    {
        requireNonNull(values, "varargs 'values' is null");

        return createLongsBlock(Arrays.asList(values));
    }

    public static Block createLongsBlock(Iterable<Long> values)
    {
        BlockBuilder builder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), 100);

        for (Long value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                BIGINT.writeLong(builder, value);
            }
        }

        return builder.build();
    }

    public static Block createLongSequenceBlock(int start, int end)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(end - start);

        for (int i = start; i < end; i++) {
            BIGINT.writeLong(builder, i);
        }

        return builder.build();
    }

    public static Block createLongRepeatBlock(int value, int length)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(length);
        for (int i = 0; i < length; i++) {
            BIGINT.writeLong(builder, value);
        }
        return builder.build();
    }

    public static Block createTimestampsWithTimezoneBlock(Long... values)
    {
        BlockBuilder builder = TIMESTAMP_WITH_TIME_ZONE.createFixedSizeBlockBuilder(values.length);
        for (long value : values) {
            TIMESTAMP_WITH_TIME_ZONE.writeLong(builder, value);
        }
        return builder.build();
    }

    public static Block createBooleanSequenceBlock(int start, int end)
    {
        BlockBuilder builder = BOOLEAN.createFixedSizeBlockBuilder(end - start);

        for (int i = start; i < end; i++) {
            BOOLEAN.writeBoolean(builder, i % 2 == 0);
        }

        return builder.build();
    }

    public static Block createDoublesBlock(Double... values)
    {
        requireNonNull(values, "varargs 'values' is null");

        return createDoublesBlock(Arrays.asList(values));
    }

    public static Block createDoublesBlock(Iterable<Double> values)
    {
        BlockBuilder builder = DOUBLE.createBlockBuilder(new BlockBuilderStatus(), 100);

        for (Double value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                DOUBLE.writeDouble(builder, value);
            }
        }

        return builder.build();
    }

    public static Block createDoubleSequenceBlock(int start, int end)
    {
        BlockBuilder builder = DOUBLE.createFixedSizeBlockBuilder(end - start);

        for (int i = start; i < end; i++) {
            DOUBLE.writeDouble(builder, (double) i);
        }

        return builder.build();
    }

    public static Block createArrayBigintBlock(Iterable<? extends Iterable<Long>> values)
    {
        ArrayType arrayType = new ArrayType(BIGINT);
        BlockBuilder builder = arrayType.createBlockBuilder(new BlockBuilderStatus(), 100);

        for (Iterable<Long> value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                arrayType.writeObject(builder, createLongsBlock(value));
            }
        }

        return builder.build();
    }

    public static Block createDateSequenceBlock(int start, int end)
    {
        BlockBuilder builder = DATE.createFixedSizeBlockBuilder(end - start);

        for (int i = start; i < end; i++) {
            DATE.writeLong(builder, i);
        }

        return builder.build();
    }

    public static Block createTimestampSequenceBlock(int start, int end)
    {
        BlockBuilder builder = TIMESTAMP.createFixedSizeBlockBuilder(end - start);

        for (int i = start; i < end; i++) {
            TIMESTAMP.writeLong(builder, i);
        }

        return builder.build();
    }
}
