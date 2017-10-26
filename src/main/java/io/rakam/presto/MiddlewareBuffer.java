/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.spi.Page;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;
import io.airlift.units.DataSize;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class MiddlewareBuffer
{
    private final List<BatchRecords> batches;
    private final MiddlewareConfig config;
    private long previousFlushTimeMillisecond;
    private AtomicLong bufferRecordCount = new AtomicLong();
    private AtomicLong bufferSize = new AtomicLong();

    public MiddlewareBuffer(MiddlewareConfig middlewareConfig)
    {
        batches = new ArrayList<>();
        this.config = middlewareConfig;
    }

    public void add(BatchRecords records)
    {
        batches.add(records);
        bufferRecordCount.addAndGet(records.getTable().cellSet().stream()
                .map(Table.Cell::getValue).map(e -> e.page).mapToLong(Page::getPositionCount).sum());
        bufferSize.addAndGet(records.getTable().cellSet().stream()
                .map(Table.Cell::getValue).map(e -> e.page).mapToLong(Page::getSizeInBytes).sum());
    }

    public boolean shouldFlush()
    {
        long size = bufferRecordCount.get();
        long timelapseMillisecond = System.currentTimeMillis() - previousFlushTimeMillisecond;
        return ((size >= config.getMaxFlushRecords()) || (timelapseMillisecond >= config.getMaxFlushDuration().toMillis()))
                || bufferSize.get() > config.getMaxSize().toBytes();
    }

    public synchronized List<BatchRecords> flush()
    {
        ImmutableList<BatchRecords> flushed = ImmutableList.copyOf(batches);
        batches.clear();
        System.out.println("bufferSize is " + DataSize.succinctDataSize(bufferSize.get(), DataSize.Unit.BYTE) + " bufferRecordCount is " + bufferRecordCount);
        bufferSize = new AtomicLong();
        bufferRecordCount = new AtomicLong();
        previousFlushTimeMillisecond = System.currentTimeMillis();
        return flushed;
    }
}
