package io.rakam.presto;
/*
 * Licensed under the Rakam Incorporation
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class KafkaProducerTest
{

    public static void main(String[] args)
            throws Exception
    {
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
                "      \"_actor\": \"860905030145899\",\n" +
                "      \"imei\": \"860905030145899\",\n" +
                "      \"car_category\": \"luxury_sedan\",\n" +
                "      \"app_version\": \"8.5.3.0.8\",\n" +
                "      \"source\": \"mqtt\",\n" +
                "      \"connection_type\": \"4G\",\n" +
                "      \"city\": \"bangalore\",\n" +
                "      \"device_model\": \"Vivo V3\",\n" +
                "      \"telecom_provider\": \"Vodafone IN\",\n" +
                "      \"timestamp\": \"2017-10-31T11:28:59.032\",\n" +
                "      \"os_version\": \"22\",\n" +
                "      \"payload\": \"dapp#summaryUpdate#1509429539556#a8d00ad7dd694854a962c89515ede983#dapi_tracker#upmbC1yVlAAIrJrUtP2ZKQ==\",\n" +
                "      \"_time\": \"2017-10-31T05:58:59.32Z\",\n" +
                "      \"_shard_time\": \"2017-10-31T05:59:25.487Z\"\n" +
                "   }\n" +
                "}";

        String message1 = "{\"id\":\"1\",\"metadata\":{\"timestamp\":1508482357905,\"schema\":\"api_failure_rakam\",\"schemaVersion\":1,\"type\":\"EVENT\",\"routingKey\":{\"type\":\"simple\",\"value\":\"1508482357905\"},\"lookupKey\":null,\"tenant\":\"driverapi_instrumentation\",\"stream\":\"driverapi_instrumentation_rakam\",\"sender\":null},\"data\":{\"event\":\"ApiFailure\",\"_actor\":\"864264033112670\",\"imei\":\"864264033112670\",\"api_code\":\"5\",\"car_category\":\"luxury_sedan\",\"app_version\":\"8.5.2.0.7\",\"source\":\"1\",\"connection_type\":\"5G\",\"city\":\"delhi\",\"device_model\":\"Vivo Y21L\",\"telecom_provider\":\"Idea\",\"timestamp\":\"2017-10-20T12:19:11.053\",\"os_version\":\"22\",\"message\":\"Please check the internet connection\",\"http_error_code\":\"408\",\"_time\":\"2017-10-20T06:49:11.53Z\",\"_shard_time\":\"2017-10-20T06:52:38.481Z\",\"_collection\":\"api_failure_rakam\",\"_project\":\"dapi\",\"new_arr_int\":1}}";
        String message2 = "{\"id\":\"1\",\"metadata\":{\"timestamp\":1508482357905,\"schema\":\"api_failure_rakam\",\"schemaVersion\":1,\"type\":\"EVENT\",\"routingKey\":{\"type\":\"simple\",\"value\":\"1508482357905\"},\"lookupKey\":null,\"tenant\":\"driverapi_instrumentation\",\"stream\":\"driverapi_instrumentation_rakam\",\"sender\":null},\"data\":{\"_collection\":\"api_failure_rakam\",\"_project\":\"dapi\",\"event\":\"ApiFailure\",\"_actor\":\"864264033112670\",\"imei\":\"864264033112670\",\"api_code\":\"5\",\"car_category\":\"luxury_sedan\",\"app_version\":\"8.5.2.0.7\",\"source\":\"1\",\"connection_type\":\"4G\",\"city\":\"delhi\",\"device_model\":\"Vivo Y21L\",\"telecom_provider\":\"Idea\",\"timestamp\":\"2017-10-20T12:19:11.053\",\"os_version\":\"22\",\"message\":\"Please check the internet connection\",\"http_error_code\":\"408\",\"_time\":\"2017-10-20T06:49:11.53Z\",\"_shard_time\":\"2017-10-20T06:52:38.481Z\",\"new_arr_int\":[1,2,3,4]}}";

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = (ObjectNode) mapper.readTree(fabricEvent);
        ObjectNode data = (ObjectNode) node.get("data");

        //Assign topicName to string variable
        String topicName = "presto_test_5";

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


        for (int i = 0; i < 10; i++) {
            int randInt = random.nextInt();
            int index = Math.abs(randInt) % 10;
            data.put("_actor", String.valueOf(randInt));
            data.put("_collection", collection + "_" + index);
            node.put("data", data);

            /*producer.send(new ProducerRecord<String, String>(topicName,
                    "key+"+Integer.toString(randInt), message2));*/
            producer.send(new ProducerRecord<String, String>(topicName,
                    Integer.toString(randInt), message1));
        }


        System.out.println("Message sent successfully");
        producer.close();
    }
}
