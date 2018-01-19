/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class KafkaIntegrationTest
{
    public void testProducer()
            throws Exception
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        String record = "{ \"id\":\"1\", \"metadata\":{}, \"data\":{ \"_project\":\"presto\", \"_collection\":\"tweet\"}}";

        for (int i = 0; i < 100000; i++) {
            Thread.sleep(10);
            RecordMetadata result = producer.send(new ProducerRecord<>("test_fabric", record)).get();
        }
    }
}
