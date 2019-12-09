/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.deserialization.avro;

import com.amazonaws.services.s3.model.S3Object;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.units.DataSize;
import io.rakam.presto.CommitterConfig;
import io.rakam.presto.DatabaseHandler;
import io.rakam.presto.DuplicateHandler;
import io.rakam.presto.FieldNameConfig;
import io.rakam.presto.MemoryTracker;
import io.rakam.presto.deserialization.MessageEventTransformer;
import io.rakam.presto.deserialization.PageReader;
import io.rakam.presto.deserialization.TableData;
import io.rakam.presto.kinesis.AvroDuplicateHandler;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class AvroMessageEventTransformer<T>
        extends MessageEventTransformer<T, BinaryDecoder>
{
    private final static Logger LOGGER = Logger.get(AvroMessageEventTransformer.class);
    private final String checkpointColumn;
    private final MemoryTracker memoryTracker;
    private final DuplicateHandler duplicateHandler;

    private BinaryDecoder decoder;

    public AvroMessageEventTransformer(FieldNameConfig fieldNameConfig, CommitterConfig committerConfig, MemoryTracker memoryTracker, DatabaseHandler database)
    {
        super(fieldNameConfig, database);
        this.checkpointColumn = fieldNameConfig.getCheckpointField();
        this.memoryTracker = memoryTracker;
        if (committerConfig.getDuplicateHandlerRocksdbDirectory() == null) {
            duplicateHandler = record -> true;
        }
        else {
            duplicateHandler = new AvroDuplicateHandler<T>(fieldNameConfig, this, committerConfig.getDuplicateHandlerRocksdbDirectory(), database);
        }
    }

    @Override
    public synchronized Map<SchemaTableName, TableData> createPageTable(Iterable<T> records, Iterable<T> bulkRecords)
            throws IOException
    {
        Map<SchemaTableName, PageReader> builder = new HashMap<>();

        for (T record : records) {
            if (!duplicateHandler.isUnique(record)) {
                continue;
            }

            decoder = DecoderFactory.get().binaryDecoder(getData(record), decoder);
            decoder.skipFixed(1);

            SchemaTableName collection = extractCollection(record, decoder);

            PageReader pageBuilder = getReader(builder, collection);
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
                long totalSize = ByteBuffer.wrap(data).getLong(1);
                bulkKey = new String(data, 9, data.length - 9, UTF_8);

                object = getBulkObject(bulkKey);
                InputStreamSliceInput input = new InputStreamSliceInput(object.getObjectContent());
                LOGGER.debug("Reading bulk file %s, total size is %s", bulkKey, DataSize.succinctBytes(totalSize).toString());
                memoryTracker.reserveMemory(totalSize);

                try {
                    decoder = DecoderFactory.get().binaryDecoder(input, decoder);
                    String project = decoder.readString();

                    while (!decoder.isEnd()) {
                        String collection = decoder.readString();
                        PageReader pageBuilder = getReader(builder, new SchemaTableName(project, collection));
                        if (pageBuilder == null) {
                            continue;
                        }

                        int countOfColumns = decoder.readInt();

                        if (countOfColumns < pageBuilder.getExpectedSchema().size()) {
                            pageBuilder.setTemporarySchema(countOfColumns);
                        }

                        int recordCount = decoder.readInt();
                        for (int i = 0; i < recordCount; i++) {
                            pageBuilder.read(decoder);
                        }

                        pageBuilder.resetTemporarySchema();
                    }
                }
                finally {
                    memoryTracker.freeMemory(totalSize);
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

        return buildTable(builder);
    }

    @Override
    public PageReader<BinaryDecoder> createPageReader(List<ColumnMetadata> metadata)
    {
        return new AvroPageReader(checkpointColumn, metadata);
    }

    protected abstract S3Object getBulkObject(String bulkKey);
}
