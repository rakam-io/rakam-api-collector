/*
 * Licensed under the Rakam Incorporation
 */

package org.apache.avro.io;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.block.BlockBuilder;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.rakam.presto.deserialization.PageBuilder;
import io.rakam.presto.deserialization.PageReaderDeserializer;
import io.rakam.presto.deserialization.avro.AvroUtil;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.util.WeakIdentityHashMap;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkState;
import static io.rakam.presto.deserialization.avro.AvroUtil.convertAvroSchema;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Float.floatToIntBits;

public class AvroPageDatumReader
        implements DatumReader<Void>, PageReaderDeserializer<BinaryDecoder>
{
    private static final ThreadLocal<Map<Schema, Map<Schema, ResolvingDecoder>>> RESOLVER_CACHE =
            ThreadLocal.withInitial(() -> new WeakIdentityHashMap<>());
    private final PageBuilder builder;
    private final Thread creator = Thread.currentThread();
    // original data layout
    private Schema actualSchema;
    // the expected output
    private final Schema expectedSchema;
    private ResolvingDecoder creatorResolver = null;

    public AvroPageDatumReader(PageBuilder pageBuilder, Schema schema)
    {
        this(pageBuilder, schema, schema);
    }

    private AvroPageDatumReader(PageBuilder pageBuilder, Schema actualSchema, Schema expectedSchema)
    {
        this.builder = pageBuilder;
        this.actualSchema = actualSchema;
        this.expectedSchema = expectedSchema;
    }

    protected final ResolvingDecoder getResolver(Schema actual, Schema expected)
            throws IOException
    {
        ResolvingDecoder resolver;

        // if we use temporary `actual`, we should not cache it
        if(actual == expected) {
            Thread currThread = Thread.currentThread();
            if (currThread == creator && creatorResolver != null) {
                return creatorResolver;
            }

            Map<Schema, ResolvingDecoder> cache = RESOLVER_CACHE.get().get(actual);
            if (cache == null) {
                cache = new WeakIdentityHashMap<>();
                RESOLVER_CACHE.get().put(actual, cache);
            }

            resolver = cache.get(expected);
            if (resolver == null) {
                resolver = DecoderFactory.get().resolvingDecoder(Schema.applyAliases(actual, expected), expected, null);
                cache.put(expected, resolver);
            }

            if (currThread == creator) {
                creatorResolver = resolver;
            }
        } else {
            resolver = DecoderFactory.get().resolvingDecoder(Schema.applyAliases(actual, expected), expected, null);
        }

        return resolver;
    }

    @Override
    public void setSchema(Schema schema)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Void read(Void reuse, Decoder in)
            throws IOException
    {
        ResolvingDecoder resolver = getResolver(actualSchema, expectedSchema);
        resolver.configure(in);

        checkState(actualSchema.getFields() != null, "Not a record");
        builder.declarePosition();
        readRecord(resolver);
        if (!((BinaryDecoder) in).isEnd()) {
            resolver.drain();
        }
        return null;
    }

    protected void readRecord(ResolvingDecoder in)
            throws IOException
    {
        BinaryDecoder binaryDecoder = (BinaryDecoder) in.in;

        for (Schema.Field field : in.readFieldOrder()) {
            BlockBuilder blockBuilder = builder.getBlockBuilder(field.pos());

            // the data may be missing since it's written in a distributed manner
            if (binaryDecoder.isEnd()) {
                fill(blockBuilder.getPositionCount() + 1);
                break;
            }

            read(field.schema(), in, blockBuilder);
        }
    }

    public void fill(int currentPosition)
    {
        for (int i = 0; i < this.expectedSchema.getFields().size(); i++) {
            BlockBuilder blockBuilder = builder.getBlockBuilder(i);
            if (blockBuilder.getPositionCount() < currentPosition) {
                blockBuilder.appendNull();
            }
        }
    }

    protected void read(Schema schema, ResolvingDecoder in, BlockBuilder blockBuilder)
            throws IOException
    {
        switch (schema.getType()) {
            case UNION:
                read(schema.getTypes().get(in.readIndex()), in, blockBuilder);
                break;
            case LONG:
                blockBuilder.writeLong(in.readLong()).closeEntry();
                break;
            case STRING:
                Slice source = Slices.utf8Slice(in.readString());
                blockBuilder.writeBytes(source, 0, source.length()).closeEntry();
                break;
            case ENUM:
                blockBuilder.writeByte(in.readEnum()).closeEntry();
                break;
            case INT:
                blockBuilder.writeInt(in.readInt()).closeEntry();
                break;
            case FLOAT:
                blockBuilder.writeInt(floatToIntBits(in.readFloat())).closeEntry();
                break;
            case DOUBLE:
                blockBuilder.writeLong(doubleToLongBits(in.readDouble())).closeEntry();
                break;
            case BOOLEAN:
                blockBuilder.writeByte(in.readBoolean() ? 1 : 0).closeEntry();
                break;
            case ARRAY:
                readArray(schema.getElementType(), in, blockBuilder.beginBlockEntry());
                blockBuilder.closeEntry();
                break;
            case NULL:
                in.readNull();
                blockBuilder.appendNull();
                break;
            case MAP:
                readMap(schema.getValueType(), in, blockBuilder.beginBlockEntry());
                blockBuilder.closeEntry();
                break;
            case RECORD:
            case FIXED:
            case BYTES:
                throw new UnsupportedOperationException();
            default:
                throw new AvroRuntimeException("Unknown type: " + schema);
        }
    }

    private void readMap(Schema valueType, ResolvingDecoder in, BlockBuilder blockBuilder)
            throws IOException
    {
        long l = in.readMapStart();
        if (l > 0) {
            do {
                for (int i = 0; i < l; i++) {
                    VARCHAR.writeString(blockBuilder, in.readString());
                    read(valueType, in, blockBuilder);
                }
            }
            while ((l = in.mapNext()) > 0);
        }
    }

    private void readArray(Schema elementType, ResolvingDecoder in, BlockBuilder blockBuilder)
            throws IOException
    {
        long l = in.readArrayStart();
        if (l > 0) {
            do {
                for (long i = 0; i < l; i++) {
                    read(elementType, in, blockBuilder);
                }
            }
            while ((l = in.arrayNext()) > 0);
        }
    }

    @Override
    public void read(BinaryDecoder in, List<ColumnMetadata> tempActualSchema)
            throws IOException
    {
        if (tempActualSchema == null) {
            read(null, in);
        }
        else {
            Schema realSchema = this.actualSchema;
            this.actualSchema = convertAvroSchema(tempActualSchema);
            read(null, in);
            this.actualSchema = realSchema;
        }
    }
}
