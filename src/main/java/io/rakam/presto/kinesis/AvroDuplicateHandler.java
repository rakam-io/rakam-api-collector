/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kinesis;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.TimestampType;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.rakam.presto.DatabaseHandler;
import io.rakam.presto.Deduplicator;
import io.rakam.presto.DuplicateHandler;
import io.rakam.presto.FieldNameConfig;
import io.rakam.presto.deserialization.MessageEventTransformer;
import io.rakam.presto.deserialization.avro.AvroUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.GenericDatumReader0;
import org.apache.avro.util.Utf8;
import org.rocksdb.RocksDBException;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class AvroDuplicateHandler<T>
        implements DuplicateHandler<T>
{
    private final MessageEventTransformer transformer;
    private final Deduplicator deduplicator;
    private final GenericData.Record avroRecord;
    private final GenericDatumReader0 genericDatumReader;
    private LoadingCache<SchemaTableName, Schema> cache;
    private BinaryDecoder decoder;
    private DynamicSliceOutput dynamicSliceOutput = new DynamicSliceOutput(10);

    @Inject
    public AvroDuplicateHandler(FieldNameConfig fieldNameConfig, MessageEventTransformer transformer, File file, DatabaseHandler databaseHandler)
    {
        this.transformer = transformer;
        cache = CacheBuilder.newBuilder().expireAfterAccess(5, TimeUnit.HOURS).build(new CacheLoader<SchemaTableName, Schema>()
        {
            @Override
            public Schema load(SchemaTableName table)
                    throws Exception
            {
                List<ColumnMetadata> columns = databaseHandler.getColumns(table.getSchemaName(), table.getTableName());
                columns = columns.stream()
                        .filter(a -> !a.getName().equals(fieldNameConfig.getCheckpointField()))
                        .collect(Collectors.toList());

                return AvroUtil.convertAvroSchema(columns);
            }
        });

        try {
            deduplicator = new Deduplicator(file);
        }
        catch (RocksDBException e) {
            throw new IllegalStateException();
        }

        Schema expectedSchema = AvroUtil.convertAvroSchema(ImmutableList.of(
                new ColumnMetadata(fieldNameConfig.getTimeField(), TimestampType.TIMESTAMP),
                new ColumnMetadata(fieldNameConfig.getUserFieldName(), fieldNameConfig.getUserFieldType().getType())));
        avroRecord = new GenericData.Record(expectedSchema);
        genericDatumReader = new GenericDatumReader0(null, expectedSchema);
    }

    @Override
    public boolean isUnique(T record)
            throws IOException
    {
        decoder = DecoderFactory.get().binaryDecoder(transformer.getData(record), decoder);
        decoder.skipFixed(1);
        SchemaTableName table = transformer.extractCollection(record, decoder);

        Schema schema = cache.getUnchecked(table);
        genericDatumReader.setSchema(schema);
        genericDatumReader.read(avroRecord, decoder);

        Long time = (Long) avroRecord.get(0);
        Object user = avroRecord.get(1);
        if (user == null || time == null) {
            return true;
        }

        dynamicSliceOutput.writeLong(time);
        if (user instanceof Utf8) {
            dynamicSliceOutput.writeBytes(((Utf8) user).getBytes());
        }
        else if (user instanceof Number) {
            dynamicSliceOutput.writeLong(((Number) user).longValue());
        }
        else {
            return true;
        }

        byte[] key = dynamicSliceOutput.slice().getBytes();
        dynamicSliceOutput.reset();

        if (deduplicator.contains(key)) {
            return false;
        }

        deduplicator.put(key);
        return true;
    }
}
