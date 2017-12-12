/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kinesis;

import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.facebook.presto.spi.SchemaTableName;
import io.rakam.presto.DatabaseHandler;
import io.rakam.presto.FieldNameConfig;
import io.rakam.presto.deserialization.avro.AvroMessageEventTransformer;
import io.rakam.presto.S3MiddlewareConfig;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import javax.inject.Inject;

import java.io.IOException;
import java.nio.ByteBuffer;

public class KinesisMessageEventTransformer
        extends AvroMessageEventTransformer<Record>
{
    private final AmazonS3Client s3Client;
    private final S3MiddlewareConfig bulkConfig;

    @Inject
    public KinesisMessageEventTransformer(FieldNameConfig fieldNameConfig, DatabaseHandler databaseHandler, S3MiddlewareConfig bulkConfig)
    {
        super(fieldNameConfig, databaseHandler);

        this.bulkConfig = bulkConfig;
        s3Client = new AmazonS3Client(bulkConfig.getCredentials());
        s3Client.setRegion(bulkConfig.getAWSRegion());
        if (bulkConfig.getEndpoint() != null) {
            s3Client.setEndpoint(bulkConfig.getEndpoint());
        }
    }

    @SuppressWarnings("Duplicates")
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
        } else
        if (dataFormatType == 2) {
            if (decoder == null) {
                decoder = DecoderFactory.get().binaryDecoder(array, null);
            }
            collection = decoder.readString();
        }
        else {
            throw new IllegalArgumentException("Unknown data format type: " + dataFormatType);
        }

        return new SchemaTableName(project, collection);
    }

    @Override
    public byte[] getData(Record record)
    {
        ByteBuffer data = record.getData();
        return data.array();
    }

    @Override
    protected S3Object getBulkObject(String bulkKey)
    {
        return s3Client.getObject(bulkConfig.getS3Bucket(), bulkKey);
    }
}
