/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.deserialization;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.TimestampType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.rakam.presto.DatabaseHandler;
import io.rakam.presto.FieldNameConfig;
import org.rakam.util.NotExistsException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;

public abstract class MessageEventTransformer<T, C> {
    static final Logger LOGGER = Logger.get(MessageEventTransformer.class);

    private final DatabaseHandler databaseHandler;
    private final FieldNameConfig fieldNameConfig;

    public MessageEventTransformer(FieldNameConfig fieldNameConfig, DatabaseHandler databaseHandler) {
        this.databaseHandler = databaseHandler;
        this.fieldNameConfig = fieldNameConfig;
    }

    public abstract SchemaTableName extractCollection(T message, @Nullable C decoder)
            throws IOException;

    public abstract byte[] getData(T record);

    public abstract Map<SchemaTableName, TableData> createPageTable(Iterable<T> records, Iterable<T> bulkRecords)
            throws IOException;

    protected PageReader generatePageBuilder(String project, String collection) {
        List<ColumnMetadata> rakamSchema;
        try {
            rakamSchema = databaseHandler.getColumns(project, collection);
        } catch (IllegalArgumentException e) {
            rakamSchema = databaseHandler.addColumns(project, collection,
                    ImmutableList.of(
                            new ColumnMetadata(fieldNameConfig.getTimeField(), TimestampType.TIMESTAMP),
                            new ColumnMetadata(fieldNameConfig.getUserFieldName(), fieldNameConfig.getUserFieldType().getType())));
        }

        if (rakamSchema == null) {
            throw new PrestoException(NOT_FOUND,
                    String.format("Source table '%s.%s' not found", project, collection));
        }

        return createPageReader(rakamSchema);
    }

    public abstract PageReader<C> createPageReader(List<ColumnMetadata> metadata);

    protected PageReader getReader(Map<SchemaTableName, PageReader> builderMap, SchemaTableName table) {
        PageReader pageBuilder = builderMap.get(table);
        if (pageBuilder == null) {
            try {
                pageBuilder = generatePageBuilder(table.getSchemaName(), table.getTableName());
            } catch (PrestoException e) {
                if (e.getErrorCode() == NOT_FOUND.toErrorCode()) {
                    LOGGER.warn(e, "Unable to find table '%s' for record.", table);
                    return null;
                } else {
                    throw e;
                }
            } catch (NotExistsException e) {
                LOGGER.warn(e, "Unable to find table '%s' for record.", table);
                return null;
            }
            builderMap.put(table, pageBuilder);
        }

        return pageBuilder;
    }

    protected Map<SchemaTableName, TableData> buildTable(Map<SchemaTableName, PageReader> builderMap) {
        ImmutableMap.Builder<SchemaTableName, TableData> builder = ImmutableMap.builder();
        for (Map.Entry<SchemaTableName, PageReader> entry : builderMap.entrySet()) {
            builder.put(entry.getKey(), new TableData(entry.getValue().buildPage(), entry.getValue().getActualSchema()));
        }
        return builder.build();
    }
}
