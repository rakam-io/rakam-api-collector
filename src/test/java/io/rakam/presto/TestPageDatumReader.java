/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.rakam.presto.deserialization.PageBuilder;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.AvroPageDatumReader;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.collect.ImmutableList.of;
import static io.rakam.presto.TestDeserializer.mapType;
import static java.lang.Float.floatToRawIntBits;
import static org.apache.avro.Schema.createRecord;
import static org.apache.avro.Schema.createUnion;

public class TestPageDatumReader
{
    private static final int ITERATION = 1000;

    @Test
    public void testComplexTypeReader()
            throws Exception
    {
        ImmutableList<AbstractType> types = of(VARCHAR, BIGINT, DATE, BOOLEAN);
        PageBuilder page = new PageBuilder(types);

        Schema schema = createRecord(
                of(new Field("test1", createUnion(of(Schema.create(Schema.Type.STRING))), null, null),
                        new Field("test2", createUnion(of(Schema.create(Schema.Type.LONG))), null, null),
                        new Field("test3", createUnion(of(Schema.create(Schema.Type.INT))), null, null),
                        new Field("test4", createUnion(of(Schema.create(Schema.Type.BOOLEAN))), null, null)
                ));

        AvroPageDatumReader reader = new AvroPageDatumReader(page, schema);

        for (int i = 0; i < ITERATION; i++) {
            GenericData.Record record = new GenericData.Record(schema);
            record.put("test1", String.valueOf(i));
            record.put("test2", (long) i);
            record.put("test3", i);
            record.put("test4", i % 2 == 0);
            byte[] bytes = get(record);
            reader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
        }

        PageAssertions.assertPageEquals(types, page.build(), new Page(BlockAssertions.createStringSequenceBlock(0, ITERATION),
                BlockAssertions.createLongSequenceBlock(0, ITERATION),
                BlockAssertions.createDateSequenceBlock(0, ITERATION),
                BlockAssertions.createBooleanSequenceBlock(0, ITERATION)));
    }

    @Test
    public void testSchemaChange()
            throws Exception
    {
        Schema schema = createRecord(
                of(new Field("test1", createUnion(of(Schema.create(Schema.Type.STRING))), null, null)));

        List<byte[]> out = new ArrayList<>(ITERATION * 2);

        for (int i = 0; i < ITERATION; i++) {
            GenericData.Record record = new GenericData.Record(schema);
            record.put("test1", String.valueOf(i));
            out.add(get(record));
        }

        schema = createRecord(
                of(new Field("test1", createUnion(of(Schema.create(Schema.Type.STRING))), null, null),
                        new Field("test2", createUnion(of(Schema.create(Schema.Type.DOUBLE))), null, null)));

        for (int i = 0; i < ITERATION; i++) {
            GenericData.Record record = new GenericData.Record(schema);
            record.put("test1", String.valueOf(i + ITERATION));
            record.put("test2", (double) i);
            out.add(get(record));
        }

        PageBuilder page = new PageBuilder(of(VARCHAR, DOUBLE));
        AvroPageDatumReader reader = new AvroPageDatumReader(page, schema);

        for (byte[] bytes : out) {
            reader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
        }

        Block stringSequenceBlock = BlockAssertions.createStringSequenceBlock(0, ITERATION * 2);
        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(new BlockBuilderStatus(), ITERATION * 2);
        for (int i = 0; i < ITERATION; i++) {
            blockBuilder.appendNull();
        }
        for (int i = 0; i < ITERATION; i++) {
            DOUBLE.writeDouble(blockBuilder, i);
        }

        PageAssertions.assertPageEquals(of(VARCHAR, DOUBLE), page.build(), new Page(stringSequenceBlock, blockBuilder.build()));
    }

    @Test
    public void testStringReader()
            throws Exception
    {
        PageBuilder page = new PageBuilder(of(VARCHAR));

        Schema schema = createRecord(of(new Field("test", createUnion(of(Schema.create(Schema.Type.STRING))), null, null)));

        AvroPageDatumReader reader = new AvroPageDatumReader(page, schema);

        readData(schema, reader);

        PageAssertions.assertPageEquals(of(VARCHAR), page.build(), new Page(BlockAssertions.createStringSequenceBlock(0, ITERATION)));
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testInvalidSchemaReader()
            throws Exception
    {
        PageBuilder page = new PageBuilder(of(BIGINT));

        Schema schema = createRecord(of(new Field("test", createUnion(of(Schema.create(Schema.Type.STRING))), null, null)));

        AvroPageDatumReader reader = new AvroPageDatumReader(page, schema);

        readData(schema, reader);
    }

    private void readData(Schema schema, AvroPageDatumReader reader)
            throws IOException
    {
        for (int i = 0; i < ITERATION; i++) {
            GenericData.Record record = new GenericData.Record(schema);
            record.put("test", String.valueOf(i));
            byte[] bytes = get(record);
            reader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
        }
    }

    @Test
    public void testBigintReader()
            throws Exception
    {
        PageBuilder page = new PageBuilder(of(BIGINT));

        Schema schema = createRecord(of(new Field("test", createUnion(of(Schema.create(Schema.Type.LONG))), null, null)));

        AvroPageDatumReader reader = new AvroPageDatumReader(page, schema);

        for (long i = 0; i < ITERATION; i++) {
            GenericData.Record record = new GenericData.Record(schema);
            record.put("test", i);
            byte[] bytes = get(record);
            reader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
        }

        PageAssertions.assertPageEquals(of(BIGINT), page.build(), new Page(BlockAssertions.createLongSequenceBlock(0, ITERATION)));
    }

    @Test
    public void testBooleanReader()
            throws Exception
    {
        PageBuilder page = new PageBuilder(of(BOOLEAN));

        Schema schema = createRecord(of(new Field("test", createUnion(of(Schema.create(Schema.Type.BOOLEAN))), null, null)));

        AvroPageDatumReader reader = new AvroPageDatumReader(page, schema);

        for (int i = 0; i < ITERATION; i++) {
            GenericData.Record record = new GenericData.Record(schema);
            record.put("test", i % 2 == 0);
            byte[] bytes = get(record);
            reader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
        }

        PageAssertions.assertPageEquals(of(BOOLEAN), page.build(), new Page(BlockAssertions.createBooleanSequenceBlock(0, ITERATION)));
    }

    @Test
    public void testDoubleReader()
            throws Exception
    {
        PageBuilder page = new PageBuilder(of(DOUBLE));

        Schema schema = createRecord(of(new Field("test", createUnion(of(Schema.create(Schema.Type.DOUBLE))), null, null)));

        AvroPageDatumReader reader = new AvroPageDatumReader(page, schema);

        for (double i = 0; i < ITERATION; i++) {
            GenericData.Record record = new GenericData.Record(schema);
            record.put("test", i);
            byte[] bytes = get(record);
            reader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
        }

        PageAssertions.assertPageEquals(of(DOUBLE), page.build(), new Page(BlockAssertions.createDoubleSequenceBlock(0, ITERATION)));
    }

    @Test
    public void testDateReader()
            throws Exception
    {
        PageBuilder page = new PageBuilder(of(DATE));

        Schema schema = createRecord(of(new Field("test", createUnion(of(Schema.create(Schema.Type.INT))), null, null)));

        AvroPageDatumReader reader = new AvroPageDatumReader(page, schema);

        for (int i = 0; i < ITERATION; i++) {
            GenericData.Record record = new GenericData.Record(schema);
            record.put("test", i);
            byte[] bytes = get(record);
            reader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
        }

        PageAssertions.assertPageEquals(of(DATE), page.build(), new Page(BlockAssertions.createDateSequenceBlock(0, ITERATION)));
    }

    @Test
    public void testArrayReader()
            throws Exception
    {
        PageBuilder page = new PageBuilder(of(new ArrayType(VARCHAR)));

        Schema array = Schema.createArray(Schema.create(Schema.Type.STRING));
        Schema schema = createRecord(of(new Field("test", createUnion(of(array)), null, null)));

        AvroPageDatumReader reader = new AvroPageDatumReader(page, schema);

        for (int i = 0; i < ITERATION; i++) {
            GenericData.Record record = new GenericData.Record(schema);
            record.put("test", new GenericData.Array(array, ImmutableList.of(String.valueOf(i), String.valueOf(i + 1))));
            byte[] bytes = get(record);
            reader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
        }

        List<ImmutableList<String>> values = IntStream.range(0, ITERATION)
                .mapToObj(i -> ImmutableList.of(String.valueOf(i), String.valueOf(i + 1)))
                .collect(Collectors.toList());

        PageAssertions.assertPageEquals(of(new ArrayType(VARCHAR)), page.build(),
                new Page(BlockAssertions.createStringArraysBlock(values)));
    }

    @Test
    public void testMapReader()
            throws Exception
    {
        MapType mapType = mapType(VARCHAR, VARCHAR);
        PageBuilder page = new PageBuilder(of(mapType));

        Schema elementType = Schema.create(Schema.Type.STRING);
        Schema schema = createRecord(of(new Field("test", createUnion(of(Schema.createMap(elementType))), null, null)));

        AvroPageDatumReader reader = new AvroPageDatumReader(page, schema);

        for (int i = 0; i < ITERATION; i++) {
            GenericData.Record record = new GenericData.Record(schema);
            record.put("test", ImmutableMap.of(String.valueOf(i), String.valueOf(i + 1), String.valueOf(i + 1), String.valueOf(i + 2)));
            byte[] bytes = get(record);
            reader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
        }

        BlockBuilder blockBuilder = mapType.createBlockBuilder(new BlockBuilderStatus(), ITERATION);

        for (int i = 0; i < ITERATION; i++) {
            mapType.writeObject(blockBuilder, mapBlockOf(VARCHAR, VARCHAR,
                    ImmutableMap.of(String.valueOf(i), String.valueOf(i + 1), String.valueOf(i + 1), String.valueOf(i + 2))));
        }

        PageAssertions.assertPageEquals(of(mapType), page.build(), new Page(blockBuilder.build()));
    }

    private static byte[] get(GenericRecord record)
            throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(out, null);

        GenericDatumWriter write = new GenericDatumWriter(record.getSchema());
        write.write(record, binaryEncoder);

        return out.toByteArray();
    }

    public static Block mapBlockOf(Type keyType, Type valueType, Map<?, ?> value)
    {
        MapType mapType = mapType(keyType, valueType);
        BlockBuilder mapArrayBuilder = mapType.createBlockBuilder(new BlockBuilderStatus(), 1);
        BlockBuilder singleMapWriter = mapArrayBuilder.beginBlockEntry();
        for (Map.Entry<?, ?> entry : value.entrySet()) {
            appendToBlockBuilder(keyType, entry.getKey(), singleMapWriter);
            appendToBlockBuilder(valueType, entry.getValue(), singleMapWriter);
        }
        mapArrayBuilder.closeEntry();
        return mapType.getObject(mapArrayBuilder, 0);
    }

    public static void appendToBlockBuilder(Type type, Object element, BlockBuilder blockBuilder)
    {
        Class<?> javaType = type.getJavaType();
        if (element == null) {
            blockBuilder.appendNull();
        }
        else if (type.getTypeSignature().getBase().equals(StandardTypes.ARRAY) && element instanceof Iterable<?>) {
            BlockBuilder subBlockBuilder = blockBuilder.beginBlockEntry();
            for (Object subElement : (Iterable<?>) element) {
                appendToBlockBuilder(type.getTypeParameters().get(0), subElement, subBlockBuilder);
            }
            blockBuilder.closeEntry();
        }
        else if (type.getTypeSignature().getBase().equals(StandardTypes.ROW) && element instanceof Iterable<?>) {
            BlockBuilder subBlockBuilder = blockBuilder.beginBlockEntry();
            int field = 0;
            for (Object subElement : (Iterable<?>) element) {
                appendToBlockBuilder(type.getTypeParameters().get(field), subElement, subBlockBuilder);
                field++;
            }
            blockBuilder.closeEntry();
        }
        else if (type.getTypeSignature().getBase().equals(StandardTypes.MAP) && element instanceof Map<?, ?>) {
            BlockBuilder subBlockBuilder = blockBuilder.beginBlockEntry();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) element).entrySet()) {
                appendToBlockBuilder(type.getTypeParameters().get(0), entry.getKey(), subBlockBuilder);
                appendToBlockBuilder(type.getTypeParameters().get(1), entry.getValue(), subBlockBuilder);
            }
            blockBuilder.closeEntry();
        }
        else if (javaType == boolean.class) {
            type.writeBoolean(blockBuilder, (Boolean) element);
        }
        else if (javaType == long.class) {
            if (element instanceof SqlDecimal) {
                type.writeLong(blockBuilder, ((SqlDecimal) element).getUnscaledValue().longValue());
            }
            else if (REAL.equals(type)) {
                type.writeLong(blockBuilder, floatToRawIntBits(((Number) element).floatValue()));
            }
            else {
                type.writeLong(blockBuilder, ((Number) element).longValue());
            }
        }
        else if (javaType == double.class) {
            type.writeDouble(blockBuilder, ((Number) element).doubleValue());
        }
        else if (javaType == Slice.class) {
            if (element instanceof String) {
                type.writeSlice(blockBuilder, Slices.utf8Slice(element.toString()));
            }
            else if (element instanceof byte[]) {
                type.writeSlice(blockBuilder, Slices.wrappedBuffer((byte[]) element));
            }
            else if (element instanceof SqlDecimal) {
                type.writeSlice(blockBuilder, Decimals.encodeUnscaledValue(((SqlDecimal) element).getUnscaledValue()));
            }
            else {
                type.writeSlice(blockBuilder, (Slice) element);
            }
        }
        else {
            type.writeObject(blockBuilder, element);
        }
    }
}
