/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.deserialization.avro;

import com.amazonaws.services.s3.model.S3Object;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.slice.InputStreamSliceInput;
import io.rakam.presto.FieldNameConfig;
import io.rakam.presto.DatabaseHandler;
import io.rakam.presto.deserialization.MessageEventTransformer;
import io.rakam.presto.deserialization.PageReader;
import io.rakam.presto.deserialization.TableData;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class AvroMessageEventTransformer<T>
        extends MessageEventTransformer<T, BinaryDecoder>
{
    private final static Logger LOGGER = Logger.get(AvroMessageEventTransformer.class);
    private final String checkpointColumn;

    private BinaryDecoder decoder;

    public AvroMessageEventTransformer(FieldNameConfig fieldNameConfig, DatabaseHandler database)
    {
        super(fieldNameConfig, database);
        this.checkpointColumn = fieldNameConfig.getCheckpointField();
    }

    @Override
    public synchronized Map<SchemaTableName, TableData> createPageTable(Iterable<T> records, Iterable<T> bulkRecords, Iterable<T> pageRecords)
            throws IOException
    {
        Map<SchemaTableName, PageReader> builderMap = new HashMap<>();

        for (T record : records) {
            decoder = DecoderFactory.get().binaryDecoder(getData(record), decoder);
            decoder.skipFixed(1);

            SchemaTableName collection = extractCollection(record, decoder);

            PageReader pageBuilder = getReader(builderMap, collection);
            if (pageBuilder == null) {
                continue;
            }

            try {
                pageBuilder.read(decoder);
            }
            catch (Exception e) {
                LOGGER.error(e, "Unable to parse message in broker.");
                return ImmutableMap.of();
            }
        }

        for (T record : bulkRecords) {
            String bulkKey = null;
            S3Object object = null;
            try {
                byte[] data = getData(record);
                bulkKey = new String(data, 9, data.length - 9, UTF_8);
                object = getBulkObject(bulkKey);
                InputStreamSliceInput input = new InputStreamSliceInput(object.getObjectContent());

                SchemaTableName table = extractCollection(record, null);

                PageReader pageBuilder = getReader(builderMap, table);
                if (pageBuilder == null) {
                    continue;
                }

                decoder = DecoderFactory.get().binaryDecoder(input, decoder);

                int countOfColumns = decoder.readInt();
                ImmutableList.Builder<ColumnMetadata> expectedSchemaBuilder = ImmutableList.builder();

                for (int i = 0; i < countOfColumns; i++) {
                    String columnName = decoder.readString();
                    Optional<ColumnMetadata> column = pageBuilder.getExpectedSchema().stream()
                            .filter(new Predicate<ColumnMetadata>()
                            {
                                @Override
                                public boolean test(ColumnMetadata o)
                                {
                                    return o.getName().equals(columnName);
                                }
                            })
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
                LOGGER.error(e, "Error while reading batch data: %s", bulkKey);
            }
            finally {
                if (object != null) {
                    object.close();
                }
            }
        }

        ImmutableMap.Builder<SchemaTableName, TableData> builder = ImmutableMap.builder();
        for (Map.Entry<SchemaTableName, PageReader> entry : builderMap.entrySet()) {
            builder.put(entry.getKey(), new TableData(entry.getValue().getPage(), entry.getValue().getActualSchema()));
        }

        return builder.build();
    }

    @Override
    public PageReader<BinaryDecoder> createPageReader(List<ColumnMetadata> metadata)
    {
        return new AvroPageReader(checkpointColumn, metadata);
    }

    protected abstract S3Object getBulkObject(String bulkKey);
}
