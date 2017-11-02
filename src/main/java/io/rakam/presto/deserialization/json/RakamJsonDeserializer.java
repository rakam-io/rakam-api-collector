/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.deserialization.json;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import com.google.common.collect.ImmutableList;
import io.rakam.presto.DatabaseHandler;
import io.rakam.presto.deserialization.PageBuilder;
import io.rakam.presto.deserialization.PageReader;
import org.rakam.collection.FieldType;
import org.rakam.presto.analysis.PrestoQueryExecution;
import org.rakam.util.DateTimeUtils;
import org.rakam.util.JsonHelper;

import java.io.IOException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.fasterxml.jackson.core.JsonToken.*;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.rakam.collection.FieldType.*;
import static org.rakam.presto.analysis.PrestoRakamRaptorMetastore.toType;

public class RakamJsonDeserializer implements JsonDeserializer
{
    private static final JsonFactory READER = new ObjectMapper().getFactory();
    private final DatabaseHandler databaseHandler;

    private Map<Type, FieldType> typeCache = new ConcurrentHashMap<>();
    private String project;
    private String collection;
    private JsonParser jp;
    TokenBuffer propertiesBuffer = null;

    public RakamJsonDeserializer(DatabaseHandler databaseHandler)
    {
        this.databaseHandler = databaseHandler;
    }

    @Override
    public void setData(byte[] data)
            throws IOException
    {
        project = null;
        collection = null;
        propertiesBuffer = null;
        this.jp = READER.createParser(data);
    }

    @Override
    public SchemaTableName getTable()
            throws IOException
    {
        if (project == null || collection == null) {
            deserialize(null);
        }
        return new SchemaTableName(project, collection);
    }

    @Override
    public void deserialize(JsonPageReader pageReader)
            throws IOException
    {
        if (project != null && collection != null) {
            parseProperties(pageReader);
            return;
        }

        JsonToken t = jp.nextToken();
        if (t == JsonToken.START_OBJECT) {
            t = jp.nextToken();
        }
        else {
            throw new IllegalArgumentException("Invalid json");
        }

        for (; t == JsonToken.FIELD_NAME; t = jp.nextToken()) {
            String fieldName = jp.getCurrentName();

            t = jp.nextToken();

            switch (fieldName) {
                case "project":
                    project = jp.getValueAsString();
                    if (project == null || project.isEmpty()) {
                        throw new RuntimeException("Project can't be null");
                    }
                    project = project.toLowerCase();
                    break;
                case "collection":
                    collection = checkCollectionValid(jp.getValueAsString());
                    break;
                case "properties":
                    if (t != START_OBJECT) {
                        throw new IllegalArgumentException("properties must be an object");
                    }

                    if (project == null || collection == null) {
                        propertiesBuffer = jp.readValueAs(TokenBuffer.class);
                    }
                    else {
                        if (pageReader != null) {
                            parseProperties(pageReader);
                        }
                        else {
                            return;
                        }
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unknown field");
            }
        }
    }

    private void parseProperties(PageReader pageReader)
            throws IOException
    {
        List<ColumnMetadata> columns = pageReader.getExpectedSchema();
        List<ColumnMetadata> newFields = null;
        PageBuilder pageBuilder = pageReader.getPageBuilder();
        pageBuilder.declarePosition();
        int currentPosition = pageBuilder.getPositionCount();

        JsonParser jp;
        if (propertiesBuffer != null) {
            jp = propertiesBuffer.asParser(this.jp);
        }
        else {
            jp = this.jp;
        }

        JsonToken t = jp.nextToken();
        for (; t == JsonToken.FIELD_NAME; t = jp.nextToken()) {
            String fieldName = jp.getCurrentName();

            int idx = -1;
            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i).getName().equals(fieldName)) {
                    idx = i;
                    break;
                }
            }

            jp.nextToken();

            if (idx == -1) {
                FieldType type = getTypeForUnknown(jp);
                if (type != null) {
                    if (newFields == null) {
                        newFields = new ArrayList<>();
                    }

                    ColumnMetadata newField = new ColumnMetadata(fieldName, toType(type));

                    pageBuilder = pageBuilder.newPageBuilderWithType(toType(type));
                    pageReader.setPageBuilder(pageBuilder);
                    columns = ImmutableList.<ColumnMetadata>builder()
                            .addAll(columns)
                            .add(newField)
                            .build();

                    newFields.add(newField);
                    idx = columns.size() - 1;

                    if (type.isArray() || type.isMap()) {
                        // if the type of new field is ARRAY, we already switched to next token
                        // so current token is not START_ARRAY.
                        getValue(pageBuilder.getBlockBuilder(idx), jp, type, newField, true);
                    }
                    else {
                        getValue(pageBuilder.getBlockBuilder(idx), jp, type, newField, false);
                    }
                }
                else {
                    // the type is null or an empty array, pass it
                    t = jp.getCurrentToken();
                }
            }
            else {
                Type type = columns.get(idx).getType();
                FieldType fieldType = typeCache.get(type);
                if (fieldType == null) {
                    TypeSignature typeSignature = type.getTypeSignature();
                    fieldType = PrestoQueryExecution.fromPrestoType(typeSignature.getBase(),
                            typeSignature.getParameters().stream().map(e -> e.toString()).iterator());
                    typeCache.put(type, fieldType);
                }

                ColumnMetadata columnMetadata = columns.get(idx);

                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(idx);
                if (currentPosition == blockBuilder.getPositionCount()) {
                    jp.skipChildren();
                }
                else {
                    getValue(blockBuilder, jp, fieldType, columnMetadata, false);
                }
            }
        }

        for (int i = 0; i < columns.size(); i++) {
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
            if (blockBuilder.getPositionCount() != currentPosition) {
                blockBuilder.appendNull();
            }
        }

        if (newFields != null) {
            pageReader.setActualSchema(databaseHandler.addColumns(project, collection, newFields));
            List<ColumnMetadata> newColumns = pageReader.getExpectedSchema();

            if (!newColumns.equals(columns)) {
                BlockBuilder[] blockBuilders = new BlockBuilder[newColumns.size()];

                for (int i = 0; i < newColumns.size(); i++) {
                    ColumnMetadata newColumn = newColumns.get(i);

                    for (int i1 = 0; i1 < columns.size(); i1++) {
                        if (columns.get(i1).getName().equals(newColumn.getName())) {
                            blockBuilders[i] = pageBuilder.getBlockBuilder(i1);
                            break;
                        }
                    }
                }

                for (int i = 0; i < blockBuilders.length; i++) {
                    if (blockBuilders[i] == null) {
                        BlockBuilder blockBuilder = newColumns.get(i).getType().createBlockBuilder(new BlockBuilderStatus(), pageBuilder.getPositionCount());
                        for (int i1 = 0; i1 < pageBuilder.getPositionCount(); i1++) {
                            blockBuilder.appendNull();
                        }
                    }
                }

                pageBuilder = new PageBuilder(pageBuilder.getPositionCount(),
                        newColumns.stream().map(e -> e.getType()).collect(Collectors.toList()),
                        Optional.ofNullable(blockBuilders), pageBuilder.getPositionCount());

                pageReader.setPageBuilder(pageBuilder);
            }
        }

        if (t != END_OBJECT) {
            if (t == JsonToken.START_OBJECT) {
                throw new IllegalArgumentException("Nested properties are not supported.");
            }
            else {
                throw new IllegalArgumentException("Error while de-serializing event");
            }
        }
    }

    public static String checkCollectionValid(String collection)
    {
        checkArgument(collection != null, "collection is null");
        checkArgument(!collection.isEmpty(), "collection is empty string");
        if (collection.length() > 100) {
            throw new IllegalArgumentException("Collection name must have maximum 250 characters.");
        }
        return collection;
    }

    private void getValue(BlockBuilder blockBuilder, JsonParser jp, FieldType type, ColumnMetadata field, boolean passInitialToken)
            throws IOException
    {
        if (jp.getCurrentToken().isScalarValue() && !passInitialToken) {
            if (jp.getCurrentToken() == VALUE_NULL) {
                blockBuilder.appendNull();
                return;
            }

            switch (type) {
                case STRING:
                    String valueAsString = jp.getValueAsString();
                    if (valueAsString.length() > 100) {
                        valueAsString = valueAsString.substring(0, 100);
                    }
                    VARCHAR.writeString(blockBuilder, valueAsString);
                    break;
                case BOOLEAN:
                    BOOLEAN.writeBoolean(blockBuilder, jp.getValueAsBoolean());
                    break;
                case LONG:
                    BIGINT.writeLong(blockBuilder, jp.getValueAsLong());
                    break;
                case INTEGER:
                    INTEGER.writeLong(blockBuilder, jp.getValueAsInt());
                    break;
                case TIME:
                    try {
                        long l = (long) LocalTime.parse(jp.getValueAsString()).getSecond();
                        TIME.writeLong(blockBuilder, l);
                    }
                    catch (Exception e) {
                        blockBuilder.appendNull();
                    }
                    break;
                case DOUBLE:
                    DoubleType.DOUBLE.writeDouble(blockBuilder, jp.getValueAsDouble());
                    break;
                case DECIMAL:
                    // TODO
                    blockBuilder.appendNull();
                    break;
                case DATE:
                    if (jp.getValueAsString().isEmpty()) {
                        blockBuilder.appendNull();
                    }
                    else if (jp.getCurrentToken().isNumeric()) {
                        blockBuilder.appendNull();
                    }
                    else {
                        try {
                            DateType.DATE.writeLong(blockBuilder, DateTimeUtils.parseDate(jp.getValueAsString()));
                        }
                        catch (Exception e) {
                            blockBuilder.appendNull();
                        }
                    }

                    break;
                case TIMESTAMP:
                    if (jp.getValueAsString().isEmpty()) {
                        blockBuilder.appendNull();
                    }
                    else if (jp.getCurrentToken().isNumeric()) {
                        blockBuilder.appendNull();
                    }
                    else {
                        try {
                            TIMESTAMP.writeLong(blockBuilder, DateTimeUtils.parseTimestamp(jp.getValueAsString()));
                        }
                        catch (Exception e) {
                            blockBuilder.appendNull();
                        }
                    }
                    break;
                default:
                    throw new JsonMappingException(jp, format("Scalar value '%s' cannot be cast to %s type for '%s' field.",
                            jp.getValueAsString(), type.name(), field.getName()));
            }
        }
        else {
            if (type.isMap()) {
                JsonToken t = jp.getCurrentToken();

                BlockBuilder mapElementBlockBuilder = blockBuilder.beginBlockEntry();

                if (!passInitialToken) {
                    if (t != JsonToken.START_OBJECT) {
                        jp.skipChildren();
                        blockBuilder.appendNull();
                        return;
                    }
                    else {
                        t = jp.nextToken();
                    }
                }
                else {
                    // In order to determine the value type of map, getTypeForUnknown method performed an extra
                    // jp.nextToken() so the cursor should be at VALUE_STRING token.
                    String key = jp.getCurrentName();

                    VARCHAR.writeString(mapElementBlockBuilder, key);

                    if (t.isScalarValue()) {
                        getValue(mapElementBlockBuilder, jp, type.getMapValueType(), null, false);
                    }
                    else {
                        String value = JsonHelper.encode(jp.readValueAsTree());
                        VARCHAR.writeString(mapElementBlockBuilder, value);
                    }

                    t = jp.nextToken();
                }

                for (; t == JsonToken.FIELD_NAME; t = jp.nextToken()) {
                    String key = jp.getCurrentName();

                    VARCHAR.writeString(mapElementBlockBuilder, key);

                    if (!jp.nextToken().isScalarValue()) {
                        if (type.getMapValueType() != STRING) {
                            throw new JsonMappingException(jp, String.format("Nested properties are not supported if the type is not MAP_STRING. ('%s' field)", field.getName()));
                        }
                        String value = JsonHelper.encode(jp.readValueAsTree());

                        VARCHAR.writeString(mapElementBlockBuilder, value);
                    }
                    else {
                        getValue(mapElementBlockBuilder, jp, type.getMapValueType(), null, false);
                    }
                }

                blockBuilder.closeEntry();
            }
            else if (type.isArray()) {
                JsonToken t = jp.getCurrentToken();
                // if the passStartArrayToken is true, we already performed jp.nextToken
                // so there is no need to check if the current token is START_ARRAY
                if (!passInitialToken) {
                    if (t != JsonToken.START_ARRAY) {
                        blockBuilder.appendNull();
                        return;
                    }
                    else {
                        t = jp.nextToken();
                    }
                }

                BlockBuilder arrayElementBlockBuilder = blockBuilder.beginBlockEntry();

                for (; t != JsonToken.END_ARRAY; t = jp.nextToken()) {
                    if (!t.isScalarValue()) {
                        if (type.getArrayElementType() != STRING) {
                            throw new JsonMappingException(jp, String.format("Nested properties are not supported if the type is not MAP_STRING. ('%s' field)", field.getName()));
                        }

                        VARCHAR.writeString(arrayElementBlockBuilder, JsonHelper.encode(jp.readValueAsTree()));
                    }
                    else {
                        getValue(arrayElementBlockBuilder, jp, type.getArrayElementType(), null, false);
                    }
                }

                blockBuilder.closeEntry();
            }
            else {
                if (type == STRING) {
                    VARCHAR.writeString(blockBuilder, JsonHelper.encode(jp.readValueAs(TokenBuffer.class)));
                }
                else {
                    jp.skipChildren();
                }
            }
        }
    }

    private static FieldType getTypeForUnknown(JsonParser jp)
            throws IOException
    {
        switch (jp.getCurrentToken()) {
            case VALUE_NULL:
                return null;
            case VALUE_STRING:
                String value = jp.getValueAsString();

                try {
                    DateTimeUtils.parseDate(value);
                    return FieldType.DATE;
                }
                catch (Exception e) {

                }

                try {
                    DateTimeUtils.parseTimestamp(value);
                    return FieldType.TIMESTAMP;
                }
                catch (Exception e) {

                }

                return STRING;
            case VALUE_FALSE:
                return FieldType.BOOLEAN;
            case VALUE_NUMBER_FLOAT:
            case VALUE_NUMBER_INT:
                return FieldType.DOUBLE;
            case VALUE_TRUE:
                return FieldType.BOOLEAN;
            case START_ARRAY:
                JsonToken t = jp.nextToken();
                if (t == JsonToken.END_ARRAY) {
                    // if the array is null, return null as value.
                    // TODO: if the key already has a type, return that type instead of null.
                    return null;
                }

                FieldType type;
                if (t.isScalarValue()) {
                    type = getTypeForUnknown(jp);
                }
                else {
                    type = MAP_STRING;
                }
                if (type == null) {
                    // TODO: what if the other values are not null?
                    while (t != END_ARRAY) {
                        if (!t.isScalarValue()) {
                            return ARRAY_STRING;
                        }
                        else {
                            t = jp.nextToken();
                        }
                    }
                    return null;
                }
                if (type.isArray() || type.isMap()) {
                    return ARRAY_STRING;
                }
                return type.convertToArrayType();
            case START_OBJECT:
                t = jp.nextToken();
                if (t == JsonToken.END_OBJECT) {
                    // if the map is null, return null as value.
                    // TODO: if the key already has a type, return that type instead of null.
                    return null;
                }
                if (t != JsonToken.FIELD_NAME) {
                    throw new IllegalArgumentException();
                }
                t = jp.nextToken();
                if (!t.isScalarValue()) {
                    return MAP_STRING;
                }
                type = getTypeForUnknown(jp);
                if (type == null) {
                    // TODO: what if the other values are not null?
                    while (t != END_OBJECT) {
                        if (!t.isScalarValue()) {
                            return MAP_STRING;
                        }
                        else {
                            t = jp.nextToken();
                        }
                    }
                    jp.nextToken();

                    return null;
                }

                if (type.isArray() || type.isMap()) {
                    return MAP_STRING;
                }
                return type.convertToMapValueType();
            default:
                throw new JsonMappingException(jp, format("The type is not supported: %s", jp.getValueAsString()));
        }
    }
}
