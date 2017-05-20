/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import com.facebook.presto.spi.Page;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import io.rakam.presto.StreamWorkerContext;
import kafka.javaapi.consumer.ConsumerConnector;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;

public class KafkaCommitter
{
    private final Table<String, String, Page> pages;
    private final KafkaMemoryBuffer buffer;
    private final StreamWorkerContext context;
    private final ConsumerConnector consumer;

    public KafkaCommitter(ConsumerConnector consumer, KafkaMemoryBuffer buffer, StreamWorkerContext context)
    {
        this.pages = HashBasedTable.create();
        this.consumer = consumer;
        this.buffer = buffer;
        this.context = context;
    }

    public synchronized void reset()
    {
        pages.clear();
        buffer.flush();
    }

    public synchronized boolean commitRecords(String project, String collection, List records)
            throws IOException, BrokenBarrierException, InterruptedException
    {
        if (pages.contains(project, collection)) {
            return false;
        }

//        Page page = context.process(records, project, collection);

//        pages.put(project, collection, page);
        if (pages.size() == buffer.getPartitionCount()) {
//            context.emit(consumer::commitOffsets, pages, "0", buffer.getFirstSequenceNumber(), buffer.getLastSequenceNumber());
            reset();
        }

        return true;
    }
}
