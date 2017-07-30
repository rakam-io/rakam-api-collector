/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import com.amazonaws.services.s3.model.S3Object;
import com.facebook.presto.spi.SchemaTableName;
import io.rakam.presto.DatabaseHandler;
import io.rakam.presto.FieldNameConfig;
import io.rakam.presto.deserialization.avro.AvroMessageEventTransformer;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.inject.Inject;

import java.io.IOException;

public class KafkaAvroMessageTransformer
        extends AvroMessageEventTransformer<ConsumerRecord<byte[], byte[]>>
{
    @Inject
    public KafkaAvroMessageTransformer(FieldNameConfig fieldNameConfig, DatabaseHandler databaseHandler)
    {
        super(fieldNameConfig, databaseHandler);
    }

    @SuppressWarnings("Duplicates")
    @Override
    public SchemaTableName extractCollection(ConsumerRecord<byte[], byte[]> message, BinaryDecoder decoder)
            throws IOException
    {
        byte[] array = message.value();

        String partitionKey = message.topic();
        int splitterIndex = partitionKey.indexOf('.');
        String project = partitionKey.substring(0, splitterIndex);
        String collection;

        byte dataFormatType = array[0];
        if (dataFormatType == 0 || dataFormatType == 1) {
            collection = partitionKey.substring(splitterIndex + 1);
        }
        else {
            if (decoder == null) {
                decoder = DecoderFactory.get().binaryDecoder(array, decoder);
            }
            collection = decoder.readString();
        }

        return new SchemaTableName(project, collection);
    }

    @Override
    public byte[] getData(ConsumerRecord<byte[], byte[]> record)
    {
        return record.value();
    }

    @Override
    protected S3Object getBulkObject(String bulkKey)
    {
        throw new UnsupportedOperationException();
    }
}
