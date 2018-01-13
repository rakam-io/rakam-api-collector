/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.rakam.presto.HistoricalDataHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.inject.Inject;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static io.rakam.presto.kafka.KafkaUtil.createProducerConfig;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class KafkaHistoricalDataHandler
        implements HistoricalDataHandler<ConsumerRecord<byte[], byte[]>>
{
    private static final Logger log = Logger.get(KafkaHistoricalDataHandler.class);

    private final KafkaProducer<byte[], byte[]> producer;
    private final String kafkaTopic;

    @Inject
    public KafkaHistoricalDataHandler(KafkaConfig kafkaConfig)
    {
        producer = new KafkaProducer<>(createProducerConfig(kafkaConfig, UUID.randomUUID().toString()));
        kafkaTopic = kafkaConfig.getHistoricalDataTopic();
//        producer.initTransactions();
    }

    @Override
    public CompletableFuture<Void> handle(Iterable<ConsumerRecord<byte[], byte[]>> table, int recordCount)
    {
        CompletableFuture<Void> future = new CompletableFuture<>();
        AtomicInteger latch = new AtomicInteger(recordCount);

//        producer.beginTransaction();

        long now = System.currentTimeMillis();
        IntegerHolder totalRecords = new IntegerHolder();
        for (ConsumerRecord<byte[], byte[]> record : table) {
            producer.send(new ProducerRecord<>(kafkaTopic, record.key(), record.value()), (metadata, exception) -> {
                if (exception != null) {
                    log.error(exception);
                }
                if (latch.decrementAndGet() == 0) {
                    log.debug("%d records are sent to Kafka historical topic in %s.", totalRecords.value,
                            Duration.succinctDuration(System.currentTimeMillis() - now, MILLISECONDS).toString());
                    future.complete(null);
                }
            });
            totalRecords.value++;
        }

        log.debug("%d records are being sent to Kafka historical topic..", totalRecords.value);

//        producer.commitTransaction();

        return future;
    }

    private static class IntegerHolder
    {
        int value;
    }
}
