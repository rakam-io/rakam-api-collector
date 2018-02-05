/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Ints;
import io.airlift.log.Logger;
import io.rakam.presto.FieldNameConfig;
import io.rakam.presto.deserialization.DecoupleMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.rakam.util.DateTimeUtils;

import javax.inject.Inject;

import java.io.IOException;
import java.time.temporal.ChronoUnit;

import static com.fasterxml.jackson.core.JsonToken.FIELD_NAME;
import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;

public class KafkaDecoupleMessage
        implements DecoupleMessage<ConsumerRecord<byte[], byte[]>>
{
    private static final Logger log = Logger.get(KafkaDecoupleMessage.class);

    private final JsonFactory factory;
    private final String timeColumn;
    private int ingestionDuration;

    @Inject
    public KafkaDecoupleMessage(FieldNameConfig fieldNameConfig)
    {
        timeColumn = fieldNameConfig.getTimeField();
        this.ingestionDuration = Ints.checkedCast(ChronoUnit.DAYS.getDuration().toMillis());
        factory = new ObjectMapper().getFactory();
    }

    @SuppressWarnings("PMD.AvoidBranchingStatementAsLastInLoop")
    public void read(ConsumerRecord<byte[], byte[]> record, RecordData recordData)
            throws IOException
    {
        JsonParser parser = factory.createParser(record.value());
        JsonToken t = parser.nextToken();
        if (t != START_OBJECT) {
            throw new JsonParseException(parser, "Must be an object");
        }
        boolean foundCollection = false;
        boolean foundProject = false;
        for (t = parser.nextToken(); t == FIELD_NAME; t = parser.nextToken()) {
            String rootFieldName = parser.getCurrentName();

            if ("metadata".equals(rootFieldName)) {
                t = parser.nextToken();

                for (t = parser.nextToken(); t == FIELD_NAME; t = parser.nextToken()) {
                    String fieldData = parser.getCurrentName();

                    if (fieldData.equals("schema")) {
                        parser.nextToken();
                        recordData.project = parser.getValueAsString();
                        foundProject = true;
                        if (foundCollection) {
                            parser.skipChildren();
                        }
                    }
                    else if (fieldData.equals("tenant")) {
                        parser.nextToken();
                        recordData.collection = parser.getValueAsString();
                        foundCollection = true;
                        if (foundProject) {
                            parser.skipChildren();
                        }
                    }
                    else {
                        parser.nextToken();
                        parser.skipChildren();
                    }
                }
            }
            else if ("data".equals(rootFieldName)) {
                t = parser.nextToken();
                if (t != START_OBJECT) {
                    throw new JsonParseException(parser, "Data object must be an object");
                }

                for (t = parser.nextToken(); t == FIELD_NAME; t = parser.nextToken()) {
                    String fieldData = parser.getCurrentName();
                    if (fieldData.equals(timeColumn)) {
                        recordData.date = findData(parser);
                        if (foundCollection) {
                            return;
                        }
                    }
                    else {
                        parser.nextToken();
                        parser.skipChildren();
                    }
                }
            }
            else {
                parser.nextToken();
            }
        }
    }

    public int findData(JsonParser parser)
            throws IOException
    {
        long eventTime;
        JsonToken t = parser.nextToken();
        switch (t) {
            case VALUE_STRING:
                eventTime = DateTimeUtils.parseTimestamp(parser.getValueAsString());
                break;
            case VALUE_NUMBER_FLOAT:
            case VALUE_NUMBER_INT:
                eventTime = DateTimeUtils.parseTimestamp(parser.getValueAsLong());
                break;
            default:
                throw new JsonParseException(parser, "Date field must be either STRING or NUMERIC");
        }

        return Ints.checkedCast(eventTime / ingestionDuration);
    }
}
