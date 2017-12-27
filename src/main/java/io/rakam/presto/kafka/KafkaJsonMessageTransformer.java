/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import com.facebook.presto.spi.SchemaTableName;
import io.rakam.presto.DatabaseHandler;
import io.rakam.presto.FieldNameConfig;
import io.rakam.presto.deserialization.json.JsonDeserializer;
import io.rakam.presto.deserialization.json.RakamJsonDeserializer;
import io.rakam.presto.deserialization.json.JsonMessageEventTransformer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.io.IOException;

public class KafkaJsonMessageTransformer
        extends JsonMessageEventTransformer<ConsumerRecord<byte[], byte[]>>
{
    @Inject
    public KafkaJsonMessageTransformer(FieldNameConfig fieldNameConfig, DatabaseHandler databaseHandler, JsonDeserializer deserializer)
    {
        super(fieldNameConfig, databaseHandler, deserializer);
    }

    @Override
    public SchemaTableName extractCollection(ConsumerRecord<byte[], byte[]> message, @Nullable JsonDeserializer decoder)
            throws IOException
    {
        jsonDecoder.setData(message.value());
        return jsonDecoder.getTable();
    }

    @Override
    public byte[] getData(ConsumerRecord<byte[], byte[]> record)
    {
        return record.value();
    }
}
