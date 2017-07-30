/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.deserialization.avro;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.apache.avro.Schema;
import org.codehaus.jackson.node.NullNode;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.avro.Schema.Type.NULL;

public class AvroUtil
{
    static {
        try {
            Field validateNames = Schema.class.getDeclaredField("validateNames");
            boolean accessible = validateNames.isAccessible();
            validateNames.setAccessible(true);
            validateNames.set(null, new ThreadLocal<Boolean>()
            {
                @Override
                public Boolean get()
                {
                    return false;
                }

                @Override
                public void set(Boolean value)
                {
                    // no-op
                    return;
                }
            });
            if (!accessible) {
                validateNames.setAccessible(false);
            }
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
    }

    public static Schema convertAvroSchema(Collection<ColumnMetadata> fields, String checkpointColumn)
    {
        List<Schema.Field> avroFields = fields.stream()
                .filter(a -> !a.getName().startsWith("$") && !a.getName().equals(checkpointColumn))
                .map(AvroUtil::generateAvroSchema).collect(Collectors.toList());

        Schema schema = Schema.createRecord("collection", null, null, false);
        schema.setFields(avroFields);

        return schema;
    }

    public static Schema.Field generateAvroSchema(ColumnMetadata field)
    {
        Schema avroSchema = getAvroSchema(field.getType().getTypeSignature());
        Schema es = Schema.createUnion(ImmutableList.of(Schema.create(NULL), avroSchema));

        return new Schema.Field(field.getName(), es, null, NullNode.getInstance());
    }

    public static Schema getAvroSchema(TypeSignature typeSignature)
    {
        switch (typeSignature.getBase()) {
            case StandardTypes.VARCHAR:
                return Schema.create(Schema.Type.STRING);
            case StandardTypes.BIGINT:
            case StandardTypes.TIME:
                return Schema.create(Schema.Type.LONG);
            case StandardTypes.VARBINARY:
                return Schema.create(Schema.Type.BYTES);
            case StandardTypes.DOUBLE:
                return Schema.create(Schema.Type.DOUBLE);
            case StandardTypes.BOOLEAN:
                return Schema.create(Schema.Type.BOOLEAN);
            case StandardTypes.DATE:
            case StandardTypes.INTEGER:
                return Schema.create(Schema.Type.INT);
            case StandardTypes.TIMESTAMP:
                return Schema.create(Schema.Type.LONG);
            case StandardTypes.ARRAY:
                Schema array = Schema.createUnion(ImmutableList.of(Schema.create(Schema.Type.NULL), getAvroSchema(typeSignature.getParameters().get(0).getTypeSignature())));
                return Schema.createArray(array);
            case StandardTypes.MAP:
                Schema map = Schema.createUnion(ImmutableList.of(Schema.create(Schema.Type.NULL), getAvroSchema(typeSignature.getParameters().get(1).getTypeSignature())));
                return Schema.createMap(map);
            default:
                throw new IllegalStateException();
        }
    }
}

