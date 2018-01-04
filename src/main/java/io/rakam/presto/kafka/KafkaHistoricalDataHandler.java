/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import com.facebook.presto.execution.buffer.PagesSerde;
import com.facebook.presto.execution.buffer.SerializedPage;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.rakam.presto.HistoricalDataHandler;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.execution.buffer.PageCompression.UNCOMPRESSED;
import static io.rakam.presto.kafka.KafkaUtil.createConsumerConfig;

public class KafkaHistoricalDataHandler
        implements HistoricalDataHandler
{
    final static byte[] KAFKA_KEY_FOR_HISTORICAL_DATA = new byte[] {0};
    private final static Logger LOGGER = Logger.get(KafkaHistoricalDataHandler.class);

    private final KafkaProducer<byte[], byte[]> producer;
    private final PagesSerde pagesSerde;
    private final String kafkaTopic;

    @Inject
    public KafkaHistoricalDataHandler(KafkaConfig kafkaConfig, BlockEncodingSerde blockEncodingSerde)
    {
        producer = new KafkaProducer<>(createConsumerConfig(kafkaConfig));
        this.kafkaTopic = kafkaConfig.getHistoricalDataTopic();
        pagesSerde = new PagesSerde(blockEncodingSerde, Optional.empty(), Optional.empty());
    }

    @Override
    public CompletableFuture<Void> handle(SchemaTableName table, List<Int2ObjectMap<Page>> pages)
    {
        CompletableFuture<Void> future = new CompletableFuture<>();
        AtomicInteger latch = new AtomicInteger(pages.stream().mapToInt(e -> e.entrySet().size()).sum());

        producer.beginTransaction();

        for (Int2ObjectMap<Page> days : pages) {
            for (Int2ObjectMap.Entry<Page> entry : days.int2ObjectEntrySet()) {
                int day = entry.getIntKey();
                Page page = entry.getValue();
                SerializedPage data = pagesSerde.serialize(page);

                int size = data.getSlice().length() + Byte.BYTES + (Integer.BYTES * 2);
                byte[] buffer = new byte[size];
                Slice slice = Slices.wrappedBuffer(buffer);
                SliceOutput output = slice.getOutput();
                output.writeByte(UNCOMPRESSED.getMarker());
                output.writeInt(data.getUncompressedSizeInBytes());
                output.writeInt(data.getPositionCount());
                output.writeBytes(data.getSlice());

                producer.send(new ProducerRecord<>(kafkaTopic, day, KAFKA_KEY_FOR_HISTORICAL_DATA, buffer), (metadata, exception) -> {
                    if (exception != null) {
                        LOGGER.error(exception);
                    }
                    if (latch.decrementAndGet() == 0) {
                        future.complete(null);
                    }
                });
            }
        }
        producer.commitTransaction();

        return future;
    }
}
