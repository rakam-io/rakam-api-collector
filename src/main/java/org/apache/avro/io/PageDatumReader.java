/*
 * Licensed under the Rakam Incorporation
 */

package org.apache.avro.io;

import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.BlockBuilder;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.util.WeakIdentityHashMap;

import java.io.IOException;
import java.util.Map;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Float.floatToIntBits;

public class PageDatumReader
        implements DatumReader<Void>
{
    private final PageBuilder builder;
    private Schema actualSchema;
    private Schema expectedSchema;

    private ResolvingDecoder creatorResolver = null;
    private final Thread creator = Thread.currentThread();

    public PageDatumReader(PageBuilder pageBuilder, Schema schema)
    {
        this(pageBuilder, schema, schema);
    }

    public PageDatumReader(PageBuilder pageBuilder, Schema actualSchema, Schema expectedSchema)
    {
        this.builder = pageBuilder;
        this.actualSchema = actualSchema;
        this.expectedSchema = expectedSchema;
    }

    private static final ThreadLocal<Map<Schema, Map<Schema, ResolvingDecoder>>> RESOLVER_CACHE =
            new ThreadLocal<Map<Schema, Map<Schema, ResolvingDecoder>>>()
            {
                protected Map<Schema, Map<Schema, ResolvingDecoder>> initialValue()
                {
                    return new WeakIdentityHashMap<>();
                }
            };

    protected final ResolvingDecoder getResolver(Schema actual, Schema expected)
            throws IOException
    {
        Thread currThread = Thread.currentThread();
        ResolvingDecoder resolver;
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

        return resolver;
    }

    @Override
    public void setSchema(Schema schema)
    {
        this.actualSchema = schema;
        if (expectedSchema == null) {
            expectedSchema = actualSchema;
        }
        creatorResolver = null;
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

    /**
     * Called to create new array instances.  Subclasses may override to use a
     * different array implementation.  By default, this returns a {@link
     * GenericData.Array}.
     */
    @SuppressWarnings("unchecked")
    protected Object newArray(int size, Schema schema)
    {
        return new GenericData.Array(size, schema);
    }
}
