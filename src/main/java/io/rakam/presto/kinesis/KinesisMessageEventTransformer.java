/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kinesis;

import com.amazonaws.services.kinesis.model.Record;
import com.facebook.presto.spi.SchemaTableName;
import io.rakam.presto.DatabaseHandler;
import io.rakam.presto.MessageEventTransformer;
import io.rakam.presto.S3MiddlewareConfig;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import javax.inject.Inject;

import java.io.IOException;
import java.nio.ByteBuffer;

public class KinesisMessageEventTransformer
        extends MessageEventTransformer<Record>
{
    @Inject
    public KinesisMessageEventTransformer(DatabaseHandler databaseHandler, S3MiddlewareConfig config)
    {
        super(databaseHandler, config);
    }

    @Override
    public SchemaTableName extractCollection(Record message, BinaryDecoder decoder)
            throws IOException
    {
        byte[] array = message.getData().array();

        String partitionKey = message.getPartitionKey();
        int splitterIndex = partitionKey.indexOf('|');
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
    public byte[] getData(Record record)
    {
        ByteBuffer data = record.getData();
        return data.array();
    }
}
