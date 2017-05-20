/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;
import io.airlift.log.Logger;
import io.airlift.slice.InputStreamSliceInput;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class MessageEventTransformer<T>
{
    static final Logger LOGGER = Logger.get(MessageEventTransformer.class);

    private final DatabaseHandler database;
    private final AmazonS3Client s3Client;
    private final S3MiddlewareConfig bulkConfig;
    private BinaryDecoder decoder;

    public MessageEventTransformer(DatabaseHandler database, S3MiddlewareConfig bulkConfig)
    {
        this.database = database;

        s3Client = new AmazonS3Client(bulkConfig.getCredentials());
        s3Client.setRegion(bulkConfig.getAWSRegion());
        if (bulkConfig.getEndpoint() != null) {
            s3Client.setEndpoint(bulkConfig.getEndpoint());
        }
        this.bulkConfig = bulkConfig;
    }

    public abstract SchemaTableName extractCollection(T message, @Nullable BinaryDecoder decoder)
            throws IOException;

    public abstract byte[] getData(T record);

    public synchronized Table<String, String, TableData> createPageTable(Iterable<T> records, Iterable<T> bulkRecords)
            throws IOException
    {
        Map<SchemaTableName, AvroPageReader> builderMap = new HashMap<>();

        for (T record : records) {
            decoder = DecoderFactory.get().binaryDecoder(getData(record), decoder);
            decoder.skipFixed(1);

            SchemaTableName collection = extractCollection(record, decoder);

            AvroPageReader pageBuilder = getReader(builderMap, collection);
            if (pageBuilder == null) {
                continue;
            }

            try {
                pageBuilder.read(decoder);
            }
            catch (Exception e) {
                LOGGER.error(e, "Unable to parse message in broker.");
                return HashBasedTable.create();
            }
        }

        for (T record : bulkRecords) {
            String s3Key = null;
            S3Object object = null;
            try {
                byte[] data = getData(record);
                s3Key = new String(data, 9, data.length - 9, UTF_8);
                object = s3Client.getObject(bulkConfig.getS3Bucket(), s3Key);
                InputStreamSliceInput input = new InputStreamSliceInput(object.getObjectContent());

                SchemaTableName table = extractCollection(record, null);

                AvroPageReader pageBuilder = getReader(builderMap, table);
                if (pageBuilder == null) {
                    continue;
                }

                decoder = DecoderFactory.get().binaryDecoder(input, decoder);

                int countOfColumns = decoder.readInt();
                ImmutableList.Builder<ColumnMetadata> expectedSchemaBuilder = ImmutableList.builder();

                for (int i = 0; i < countOfColumns; i++) {
                    String columnName = decoder.readString();
                    Optional<ColumnMetadata> column = pageBuilder.getExpectedSchema().stream()
                            .filter(c -> c.getName().equals(columnName))
                            .findAny();
                    if (!column.isPresent()) {
                        throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unknown column: " + columnName);
                    }
                    expectedSchemaBuilder.add(column.get());
                }

                ImmutableList<ColumnMetadata> build = expectedSchemaBuilder.build();
                if (build.size() > pageBuilder.getExpectedSchema().size()) {
                    throw new IllegalStateException();
                }
                else if (build.size() < pageBuilder.getExpectedSchema().size()) {
                    pageBuilder.setActualSchema(build);
                }

                int recordCount = decoder.readInt();
                for (int i = 0; i < recordCount; i++) {
                    pageBuilder.read(decoder);
                }
            }
            catch (Exception e) {
                LOGGER.error(e, "Error while reading batch data: %s", s3Key == null ? "" : s3Key);
            } finally {
                if(object != null) {
                    object.close();
                }
            }
        }

        return buildTable(builderMap);
    }

    private AvroPageReader getReader(Map<SchemaTableName, AvroPageReader> builderMap, SchemaTableName table)
    {
        AvroPageReader pageBuilder = builderMap.get(table);
        if (pageBuilder == null) {
            try {
                pageBuilder = generatePageBuilder(table.getSchemaName(), table.getTableName());
            }
            catch (PrestoException e) {
                if (e.getErrorCode() == NOT_FOUND.toErrorCode()) {
                    LOGGER.warn(e, "Unable to find table '%s' for record.", table);
                    return null;
                } else {
                    throw e;
                }
            }
            builderMap.put(table, pageBuilder);
        }

        return pageBuilder;
    }

    private Table<String, String, TableData> buildTable(Map<SchemaTableName, AvroPageReader> builderMap)
    {
        Table<String, String, TableData> table = HashBasedTable.create();
        for (Map.Entry<SchemaTableName, AvroPageReader> entry : builderMap.entrySet()) {
            SchemaTableName key = entry.getKey();
            table.put(key.getSchemaName(), key.getTableName(),
                    new TableData(entry.getValue().getPage(), entry.getValue().getExpectedSchema()));
        }

        return table;
    }

    private AvroPageReader generatePageBuilder(String project, String collection)
    {
        List<ColumnMetadata> rakamSchema = database.getColumns(project, collection);

        if (rakamSchema == null) {
            throw new PrestoException(NOT_FOUND,
                    String.format("Source table '%s.%s' not found", project, collection));
        }

        return new AvroPageReader(rakamSchema);
    }

    public static class TableData
    {
        public final Page page;
        public final List<ColumnMetadata> metadata;

        public TableData(Page page, List<ColumnMetadata> metadata)
        {
            this.page = page;
            this.metadata = metadata;
        }
    }
}
