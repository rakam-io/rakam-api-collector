/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.deserialization.json;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.Table;
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
import java.util.Set;

public abstract class JsonMessageEventTransformer<T>
        extends MessageEventTransformer<T, JsonDeserializer>
{
    static final Logger LOGGER = Logger.get(JsonMessageEventTransformer.class);
    private final JsonDeserializer jsonDecoder;
    private final String checkpointColumn;
    private Set<String> whitelistedCollections;

    public JsonMessageEventTransformer(FieldNameConfig fieldNameConfig, DatabaseHandler database)
    {
        super(fieldNameConfig, database);
        jsonDecoder = new JsonDeserializer(database, fieldNameConfig);
        this.checkpointColumn = fieldNameConfig.getCheckpointField();
        this.whitelistedCollections = fieldNameConfig.getWhitelistedCollections();
    }

    @Override
    public synchronized Table<String, String, TableData> createPageTable(Iterable<T> records, Iterable<T> bulkRecords)
            throws IOException
    {
        Map<SchemaTableName, PageReader> builderMap = new HashMap<>();
        for (T record : records) {
            SchemaTableName collection;
            try {
                collection = extractCollection(record, jsonDecoder);
                if (whitelistedCollections.size()>0 && !whitelistedCollections.contains(collection.getTableName())) {
                    continue;
                }
            }
            catch (Throwable e) {
                LOGGER.error(e, "Unable to parse collection from message in Kafka topic.");
                continue;
            }

            try {
                PageReader pageBuilder = getReader(builderMap, collection);
                if (pageBuilder == null) {
                    continue;
                }
                pageBuilder.read(jsonDecoder);
            }catch (Exception e){
                LOGGER.error(e, "Unable to parse message skipping it.");
                continue;
            }
        }

        return buildTable(builderMap);
    }

    @Override
    public PageReader<JsonDeserializer> createPageReader(List<ColumnMetadata> metadata)
    {
        return new JsonPageReader(checkpointColumn, metadata);
    }
}
