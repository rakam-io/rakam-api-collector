/*
 * Licensed under the Rakam Incorporation
 */
package io.rakam.presto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.rakam.presto.kafka.KafkaConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.joda.time.DateTime;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import static io.rakam.presto.kafka.KafkaUtil.createProducerConfig;

public class KafkaProducerTest
{

    public static void main(String[] args)
            throws Exception
    {
        DateTime beginTime = new DateTime().minusDays(3);
        DateTime endTime = new DateTime();
        Random random = new Random(10000);

        String fabricEvent = "{\n" +
                "   \"id\": \"151e05df-d1dc-4fd9-80ef-5162ddb0f229\",\n" +
                "   \"metadata\": {\n" +
                "      \"timestamp\": 1509429562188,\n" +
                "      \"schema\": \"dapi_pushmessage_event_from_app_rakam\",\n" +
                "      \"schemaVersion\": 1,\n" +
                "      \"type\": \"EVENT\",\n" +
                "      \"routingKey\": {\n" +
                "         \"type\": \"simple\",\n" +
                "         \"value\": \"1509429562188\"\n" +
                "      },\n" +
                "      \"lookupKey\": null,\n" +
                "      \"tenant\": \"driverapi_instrumentation\",\n" +
                "      \"stream\": \"driverapi_instrumentation_rakam\",\n" +
                "      \"sender\": null\n" +
                "   },\n" +
                "   \"data\": {\n" +
                "      \"_collection\": \"\",\n" +
                "      \"_project\": \"stress_test\",\n" +
                "      \"event\": \"PushMessage\",\n" +
                "      \"_actor\": \"8609050301499\",\n" +
                "      \"imei\": \"8609050301499\",\n" +
                "      \"car_category\": \"luxury_sedan\",\n" +
                "      \"app_version\": \"8.5.3.0.8\",\n" +
                "      \"source\": \"mqtt\",\n" +
                "      \"connection_type\": \"4G\",\n" +
                "      \"city\": \"bangalore\",\n" +
                "      \"device_model\": \"Vivo V3\",\n" +
                "      \"telecom_provider\": \"Vodafone IN\",\n" +
                "      \"timestamp\": \"2017-12-27T11:28:59.032\",\n" +
                "      \"os_version\": \"22\",\n" +
                "      \"payload\": \"dapp#summaryUpdate#1509429539556#a8d00ad7dd694854a962c89515ede983#dapi_tracker#upmbC1yVlAAIrJrUtP2ZKQ==\",\n" +
                "      \"_time\": \"2017-12-26T11:28:59.032\",\n" +
                "      \"_shard_time\": \"2017-10-31T05:59:25.487Z\"\n" +
                "   }\n" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = (ObjectNode) mapper.readTree(fabricEvent);
        ObjectNode data = (ObjectNode) node.get("data");

        String topic = "stress_test";
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(createProducerConfig(new KafkaConfig().setNodes("78.47.94.214:9092").setTopic(topic), null));

        String collection = "event";

        int i = 0;
        while(true) {
            int randInt = random.nextInt();
            Timestamp randomDate = new Timestamp(ThreadLocalRandom.current().nextLong(beginTime.getMillis(), endTime.getMillis()));

            data.put("_actor", String.valueOf(randInt));
            data.put("_collection", collection + (i++ % 200));
            data.put("_time", randomDate.toString());
            data.put("_shard_time", randomDate.toString());

            node.put("data", data);
            producer.send(new ProducerRecord<>(topic, null, mapper.writeValueAsBytes(node)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception)
                {
                    if(exception != null) {
                        exception.printStackTrace();
                    }
                }
            });
            Thread.sleep(100);
        }
    }
}
