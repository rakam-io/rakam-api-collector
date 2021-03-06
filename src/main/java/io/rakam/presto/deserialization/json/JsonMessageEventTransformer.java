/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.deserialization.json;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.rakam.presto.DatabaseHandler;
import io.rakam.presto.FieldNameConfig;
import io.rakam.presto.deserialization.MessageEventTransformer;
import io.rakam.presto.deserialization.PageReader;
import io.rakam.presto.deserialization.TableData;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class JsonMessageEventTransformer<T>
        extends MessageEventTransformer<T, JsonDeserializer>
{
    static final Logger LOGGER = Logger.get(JsonMessageEventTransformer.class);
    protected final JsonDeserializer jsonDecoder;
    private final String checkpointColumn;

    public JsonMessageEventTransformer(FieldNameConfig fieldNameConfig, DatabaseHandler database, JsonDeserializer jsonDecoder)
    {
        super(fieldNameConfig, database);
        this.jsonDecoder = jsonDecoder;
        this.checkpointColumn = fieldNameConfig.getCheckpointField();
    }

    @Override
    public synchronized Map<SchemaTableName, TableData> createPageTable(Iterable<T> records, Iterable<T> bulkRecords)
    {
        Map<SchemaTableName, PageReader> builder = new HashMap<>();
        for (T record : records) {
            SchemaTableName collection;
            try {
                collection = extractCollection(record, jsonDecoder);
            }
            catch (Throwable e) {
                LOGGER.error(e, "Unable to parse collection from message in Kafka topic.");
                continue;
            }

            try {
                PageReader pageBuilder = getReader(builder, collection);
                if (pageBuilder == null) {
                    continue;
                }
                pageBuilder.read(jsonDecoder);
            }
            catch (Exception e) {
                LOGGER.error(e, "Unable to parse message skipping it");
                continue;
            }
        }

        return buildTable(builder);
    }

    @Override
    public PageReader<JsonDeserializer> createPageReader(List<ColumnMetadata> metadata)
    {
        return new JsonPageReader(checkpointColumn, metadata);
    }
}