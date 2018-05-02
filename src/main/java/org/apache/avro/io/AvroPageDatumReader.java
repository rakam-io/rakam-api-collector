/*
 * Licensed under the Rakam Incorporation
 */

package org.apache.avro.io;

import com.facebook.presto.spi.block.BlockBuilder;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.rakam.presto.deserialization.PageBuilder;
import io.rakam.presto.deserialization.PageReaderDeserializer;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Float.floatToIntBits;

public class AvroPageDatumReader
        implements DatumReader<Void>, PageReaderDeserializer<BinaryDecoder>
{
    private final PageBuilder builder;
    // original data layout
    private final Schema actualSchema;
    private ResolvingDecoder creatorResolver = null;
    private ResolvingDecoder temporaryResolver = null;
    private int temporaryResolverIndex;

    public AvroPageDatumReader(PageBuilder pageBuilder, Schema schema)
    {
        this.builder = pageBuilder;
        this.actualSchema = schema;
        creatorResolver = generateResolver(schema);
    }

    protected final ResolvingDecoder getResolver()
            throws IOException
    {
        if (temporaryResolver != null) {
            return temporaryResolver;
        }
        else {
            return creatorResolver;
        }
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
        ResolvingDecoder resolver = getResolver();
        resolver.configure(in);

        builder.declarePosition();
        BinaryDecoder binaryDecoder = (BinaryDecoder) in;

        readRecord(resolver, binaryDecoder);

        if (temporaryResolver != null) {
            int totalColumns = actualSchema.getFields().size();
            for(int currentPos = temporaryResolverIndex; currentPos < totalColumns; currentPos++) {
                builder.getBlockBuilder(currentPos).appendNull();
            }
        }
        else {
            if (!binaryDecoder.isEnd()) {
                resolver.drain();
            }
        }

        return null;
    }

    protected void readRecord(ResolvingDecoder in, BinaryDecoder binaryDecoder)
            throws IOException
    {
        for (Schema.Field field : in.readFieldOrder()) {
            BlockBuilder blockBuilder = builder.getBlockBuilder(field.pos());

            // the data may be missing since it's written in a distributed manner
            // if the stream contains multiple records, this approach doesn't work so use temporaryResolver for that use-case.
            if (binaryDecoder.isEnd()) {
                fill(blockBuilder.getPositionCount() + 1);
                break;
            }

            read(field.schema(), in, blockBuilder);
        }
    }

    public void fill(int currentPosition)
    {
        for (int i = 0; i < this.actualSchema.getFields().size(); i++) {
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
    public void read(BinaryDecoder in)
            throws IOException
    {
        read(null, in);
    }

    @Override
    public void setLastColumnIndex(int lastColumnIndex)
    {
        List<Schema.Field> actualFields = actualSchema.getFields();
        List<Schema.Field> fields = new ArrayList<>(lastColumnIndex);
        for (int i = 0; i < lastColumnIndex; i++) {
            Schema.Field field = actualFields.get(i);
            fields.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue()));
        }
        this.temporaryResolver = generateResolver(Schema.createRecord(fields));
        this.temporaryResolverIndex = lastColumnIndex;
    }

    @Override
    public void resetLastColumnIndex()
    {
        this.temporaryResolver = null;
    }

    private ResolvingDecoder generateResolver(Schema schema)
    {
        try {
            return DecoderFactory.get().resolvingDecoder(Schema.applyAliases(schema, schema), schema, null);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
