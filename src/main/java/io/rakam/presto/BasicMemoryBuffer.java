/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BasicMemoryBuffer<T>
        implements MemoryBuffer<T>
{
    private final int numMessagesToBuffer;
    private final long millisecondsToBuffer;
    private final List<T> buffer;
    private final List<T> bulkBuffer;
    private int totalBytes;
    private long previousFlushTimeMillisecond;
    private long dataSizeToBuffer;

    public BasicMemoryBuffer(StreamConfig config)
    {
        numMessagesToBuffer = config.getMaxFlushRecords();
        millisecondsToBuffer = config.getMaxFlushDuration().toMillis();
        dataSizeToBuffer = config.getDataSize().toBytes();
        previousFlushTimeMillisecond = System.currentTimeMillis();
        this.buffer = new ArrayList<>(1000);
        this.bulkBuffer = new ArrayList<>(1000);
        totalBytes = 0;
    }

    public long getNumRecordsToBuffer()
    {
        return numMessagesToBuffer;
    }

    public long getMillisecondsToBuffer()
    {
        return millisecondsToBuffer;
    }

    public void consumeRecord(T record, long size)
    {
        buffer.add(record);
        totalBytes += size;
    }

    public void consumeBatch(T record, long size)
    {
        bulkBuffer.add(record);
        totalBytes += size;
    }

    public void clear()
    {
        buffer.clear();
        bulkBuffer.clear();
        previousFlushTimeMillisecond = System.currentTimeMillis();
        totalBytes = 0;
    }

    public boolean shouldFlush()
    {
        long timelapseMillisecond = System.currentTimeMillis() - previousFlushTimeMillisecond;
        return (buffer.size() >= getNumRecordsToBuffer())
                || (timelapseMillisecond >= getMillisecondsToBuffer())
                || totalBytes > dataSizeToBuffer;
    }

    public Map.Entry<List<T>, List<T>> getRecords()
    {
        return new AbstractMap.SimpleEntry<>(buffer, bulkBuffer);
    }
}
