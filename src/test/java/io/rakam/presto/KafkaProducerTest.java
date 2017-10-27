package io.rakam.presto;
/*
 * Licensed under the Rakam Incorporation
 */
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerTest
{

    public static void main(String[] args)
            throws Exception
    {

        String fabricEvent = "{\n" +
                "\t\"id\": \"d2c342a5-f488-4713-9ab4-0b5d9ad3ab0b\",\n" +
                "\t\"metadata\": {\n" +
                "\t\t\"timestamp\": 1504351744418,\n" +
                "\t\t\"schema\": \"new_screen_events_rakam\",\n" +
                "\t\t\"schemaVersion\": 1,\n" +
                "\t\t\"type\": \"EVENT\",\n" +
                "\t\t\"routingKey\": {\n" +
                "\t\t\t\"type\": null,\n" +
                "\t\t\t\"value\": \"04o3ed6LUpzTyZGR902ENwkLrX_O4WiOTQ5R_WaE4ic\"\n" +
                "\t\t},\n" +
                "\t\t\"lookupKey\": {\n" +
                "\t\t\t\"type\": null,\n" +
                "\t\t\t\"value\": \"04o3ed6LUpzTyZGR902ENwkLrX_O4WiOTQ5R_WaE4ic\"\n" +
                "\t\t},\n" +
                "\t\t\"tenant\": \"spock\",\n" +
                "\t\t\"stream\": \"new_app_session_rakam\",\n" +
                "\t\t\"sender\": null\n" +
                "\t},\n" +
                "\t\"data\": {\n" +
                "\t\t\"_collection\": \"new_screen_events_rakam_1\",\n" +
                "\t\t\"schema\": \"screen_events_rakam\",\n" +
                "\t\t\"_actor\": \"121222792\",\n" +
                "\t\t\"_project\": \"dapi\",\n" +
                "\t\t\"value\": \"capi\",\n" +
                "\t\t\"_time\": \"2017-08-02T09:47:14.519Z\",\n" +
                "\t\t\"_shard_time\": \"2017-09-02T11:29:04.494Z\",\n" +
                "\t\t\"name\": \"fabric stream\"\n" +
                "\t}\n" +
                "}";

        String fabricEvent1 = "{\n" +
                "\t\"id\": \"d2c342a5-f488-4713-9ab4-0b5d9ad3ab0b\",\n" +
                "\t\"metadata\": {\n" +
                "\t\t\"timestamp\": 1504351744418,\n" +
                "\t\t\"schema\": \"new_screen_events_rakam\",\n" +
                "\t\t\"schemaVersion\": 1,\n" +
                "\t\t\"type\": \"EVENT\",\n" +
                "\t\t\"routingKey\": {\n" +
                "\t\t\t\"type\": null,\n" +
                "\t\t\t\"value\": \"04o3ed6LUpzTyZGR902ENwkLrX_O4WiOTQ5R_WaE4ic\"\n" +
                "\t\t},\n" +
                "\t\t\"lookupKey\": {\n" +
                "\t\t\t\"type\": null,\n" +
                "\t\t\t\"value\": \"04o3ed6LUpzTyZGR902ENwkLrX_O4WiOTQ5R_WaE4ic\"\n" +
                "\t\t},\n" +
                "\t\t\"tenant\": \"spock\",\n" +
                "\t\t\"stream\": \"new_app_session_rakam\",\n" +
                "\t\t\"sender\": null\n" +
                "\t},\n" +
                "\t\"data\": {\n" +
                "\t\t\"_collection\": \"new_screen_events_rakam_2\",\n" +
                "\t\t\"schema\": \"screen_events_rakam\",\n" +
                "\t\t\"_actor\": \"121222792\",\n" +
                "\t\t\"_project\": \"dapi\",\n" +
                "\t\t\"value\": \"capi\",\n" +
                "\t\t\"_time\": \"2017-08-02T09:47:14.519Z\",\n" +
                "\t\t\"_shard_time\": \"2017-09-02T11:29:04.494Z\",\n" +
                "\t\t\"name\": \"fabric stream\"\n" +
                "\t}\n" +
                "}";
        String defaultEvent = "{  \n" +
                "   \"collection\":\"screen_events_rakam3\",\n" +
                "   \"project\":\"dapi\",\n" +
                "   \"properties\":{  \n" +
                "      \"_actor\":\"121222792\",\n" +
                "      \"_time\":\"2017-09-02T09:47:14.519Z\",     \n" +
                "\t\t\"_shard_time\": \"2017-09-02T11:29:04.494Z\",\n" +
                "      \"name\": \"test stream\"\n" +
                "   }\n" +
                "}";

        //Assign topicName to string variable
        String topicName = "presto_test_1";

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

        for (int i = 0; i >=0; i++) {
            producer.send(new ProducerRecord<String, String>(topicName,
                    Integer.toString(i), fabricEvent));

            producer.send(new ProducerRecord<String, String>(topicName,
                    Integer.toString(i), fabricEvent1));
        }
        System.out.println("Message sent successfully");
        producer.close();
    }
}
