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


       String fabricEvent1 = "{\n" +
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
                "      \"_collection\": \"dapi_pushmessage_event_from_app_rakam_1\",\n" +
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

        String fabricEvent2 = "{\n" +
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
                "      \"_collection\": \"dapi_pushmessage_event_from_app_rakam_2\",\n" +
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


        String fabricEvent3 = "{\n" +
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
                "      \"_collection\": \"dapi_pushmessage_event_from_app_rakam_3\",\n" +
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

        String fabricEvent4 = "{\n" +
                "   \"id\": \"151e05df-d1dc-4fd9-80ef-5162ddb0f229\",\n" +
                "   \"metadata\": {\n" +
                "      \"timestamp\": 1509429562188,\n" +
                "      \"schema\": \"dapi_pushmessage_event_from_app_rakam_4\",\n" +
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
                "      \"_collection\": \"dapi_pushmessage_event_from_app_rakam_4\",\n" +
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

        String fabricEvent5 = "{\n" +
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
                "      \"_collection\": \"dapi_pushmessage_event_from_app_rakam_5\",\n" +
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

        String fabricEvent6 = "{\n" +
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
                "      \"_collection\": \"dapi_pushmessage_event_from_app_rakam_6\",\n" +
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

        //Assign topicName to string variable
        String topicName = "presto_test";

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
                    Integer.toString(i), fabricEvent1));
            producer.send(new ProducerRecord<String, String>(topicName,
                    Integer.toString(i), fabricEvent2));
            producer.send(new ProducerRecord<String, String>(topicName,
                    Integer.toString(i), fabricEvent3));
            producer.send(new ProducerRecord<String, String>(topicName,
                    Integer.toString(i), fabricEvent4));
            producer.send(new ProducerRecord<String, String>(topicName,
                    Integer.toString(i), fabricEvent5));
            producer.send(new ProducerRecord<String, String>(topicName,
                    Integer.toString(i), fabricEvent6));
        }
        System.out.println("Message sent successfully");
        producer.close();
    }
}
