/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import com.facebook.presto.spi.SchemaTableName;
import io.airlift.log.Logger;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;

public class KafkaStreamConsumer
        implements Runnable
{
    private static final Logger logger = Logger.get(KafkaStreamConsumer.class);

    private final KafkaStream<byte[], byte[]> stream;
    private final KafkaMemoryBuffer.Buffer buffer;
    private final KafkaMemoryBuffer mainBuffer;
    private final String project;
    private final String collection;
    private final KafkaCommitter committer;

    public KafkaStreamConsumer(String project, String collection, KafkaStream<byte[], byte[]> stream, KafkaMemoryBuffer buffer, KafkaCommitter committer)
    {
        this.stream = stream;
        this.committer = committer;
        this.buffer = buffer.createOrGetPartitionedBuffer(new SchemaTableName(project, collection));
        this.mainBuffer = buffer;
        this.project = project;
        this.collection = collection;
    }

    @Override
    public void run()
    {
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

        while (true) {
            process(iterator);
            if (mainBuffer.shouldFlush()) {
                try {
                    boolean b = committer.commitRecords(project, collection, buffer.getRecords());
                    if (b) {
                        buffer.clear();
                    }
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                catch (BrokenBarrierException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void process(ConsumerIterator<byte[], byte[]> iterator)
    {
        int i = 0;
        long start = System.currentTimeMillis();
        while ((i < 100 && (System.currentTimeMillis() - start) < 5000) || !buffer.shouldFlush()) {
            if (hasNext(iterator)) {
                MessageAndMetadata<byte[], byte[]> messageAndMetadata = iterator.next();
                buffer.consumeRecord(messageAndMetadata, messageAndMetadata.message().length);
            }
            else {
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            i++;
        }
    }

    boolean hasNext(ConsumerIterator<byte[], byte[]> iterator)
    {
        try {
            iterator.hasNext();
            return true;
        }
        catch (ConsumerTimeoutException e) {
            return false;
        }
    }
}
