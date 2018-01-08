package io.rakam.presto;
/*
 * Licensed under the Rakam Incorporation
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.DateTime;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class KafkaProducerTest
{

    public static void main(String[] args)
            throws Exception
    {
        String timestampFormat = "yyyy-MM-dd'T'HH:mm:ss.S'Z'";
        int topicCount = 10;
        SimpleDateFormat dateFormat = new SimpleDateFormat(timestampFormat);
        DateTime beginTime = new DateTime().minusDays(2);
        DateTime endTime = new DateTime();
        Random random = new Random(10000);

        String collection = "dapi_pushmessage_event_from_app_rakam";
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
                "      \"_collection\": \"dapi_pushmessage_event_from_app_rakam\",\n" +
                "      \"_project\": \"dapi\",\n" +
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

        //Assign topicName to string variable
        String topicName = "presto_test_9";

        // create instance for properties to access producer configs
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", "localhost:9092");

        //Set acknowledgements for producer requests.
        props.put("acks", "all");

        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        boolean flag = true;

        for (int i = 0; i >= 0; i++) {
            int randInt = random.nextInt();
            int index = Math.abs(randInt) % topicCount;
            Timestamp randomDate = new Timestamp(ThreadLocalRandom.current().nextLong(beginTime.getMillis(), endTime.getMillis()));

            data.put("_actor", String.valueOf(randInt));
            data.put("_collection", collection + "_" + index);
            data.put("_time", randomDate.toString());
            data.put("_shard_time", randomDate.toString());

            node.put("data", data);
            producer.send(new ProducerRecord<String, String>(topicName,
                    Integer.toString(randInt), mapper.writeValueAsString(node)));
            Thread.sleep(1);
        }

        System.out.println("Message sent successfully");
        producer.close();
    }
}