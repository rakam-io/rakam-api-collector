/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import com.facebook.presto.spi.SchemaTableName;
import io.rakam.presto.DatabaseHandler;
import io.rakam.presto.MessageEventTransformer;
import kafka.message.MessageAndMetadata;
import org.apache.avro.io.BinaryDecoder;

import javax.inject.Inject;

public class KafkaMessageTransformer extends MessageEventTransformer<MessageAndMetadata<byte[], byte[]>>
{
    @Inject
    public KafkaMessageTransformer(DatabaseHandler databaseHandler) {
        super(databaseHandler, null);
    }

    @Override
    public SchemaTableName extractCollection(MessageAndMetadata<byte[], byte[]> message, BinaryDecoder decoder) {
        String partitionKey = message.topic();
        int splitterIndex = partitionKey.indexOf('_');
        String project = partitionKey.substring(0, splitterIndex);
        String collection = partitionKey.substring(splitterIndex + 1);
        return new SchemaTableName(project, collection);
    }

    @Override
    public byte[] getData(MessageAndMetadata<byte[], byte[]> record) {
        return record.message();
    }
}
