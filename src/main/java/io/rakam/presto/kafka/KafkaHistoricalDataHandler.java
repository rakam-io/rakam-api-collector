/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import com.facebook.presto.execution.buffer.PagesSerde;
import com.facebook.presto.execution.buffer.SerializedPage;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import io.rakam.presto.HistoricalDataHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.rakam.presto.kafka.KafkaWorkerManager.createConsumerConfig;

public class KafkaHistoricalDataHandler
        implements HistoricalDataHandler
{
    private final KafkaProducer<String, byte[]> producer;
    private final PagesSerde pagesSerde;
    private final KafkaConfig kafkaTopic;

    @Inject
    public KafkaHistoricalDataHandler(KafkaConfig kafkaConfig, BlockEncodingSerde blockEncodingSerde)
    {
        producer = new KafkaProducer<>(createConsumerConfig(kafkaConfig));
        this.kafkaTopic = kafkaConfig;
        pagesSerde = new PagesSerde(blockEncodingSerde, Optional.empty(), Optional.empty());
    }

    @Override
    public CompletableFuture<Void> handle(SchemaTableName table, List<Page> pages)
    {
        //producer.beginTransaction();

        for (Page page : pages) {
            SerializedPage value = pagesSerde.serialize(page);
            // TODO: why kafka?
            byte[] bytes = value.getSlice().getBytes();
            producer.send(new ProducerRecord<>(null, table.toString(), bytes));
        }
        //producer.commitTransaction();
        return null;
    }
}
