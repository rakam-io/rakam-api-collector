/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import java.util.ArrayList;
import java.util.List;

public class BasicMemoryBuffer<T>
        implements MemoryBuffer<T>
{
    private final long millisecondsToBuffer;
    private final ArrayList<T> buffer;
    private final ArrayList<T> bulkBuffer;
    private final SizeCalculator<T> sizeCalculator;
    private final double maxBytes;
    private long previousFlushTimeMillisecond;
    private int totalBytes;

    public BasicMemoryBuffer(StreamConfig config, SizeCalculator<T> sizeCalculator)
    {
        this.sizeCalculator = sizeCalculator;
        millisecondsToBuffer = config.getMaxFlushDuration().toMillis();
        previousFlushTimeMillisecond = System.currentTimeMillis();
        buffer = new ArrayList<>(1000);
        bulkBuffer = new ArrayList<>(1000);
        maxBytes = MemoryTracker.getAvailableHeapSize() * config.getMaxFlushTotalMemoryRatio();
        totalBytes = 0;
    }

    public long getMillisecondsToBuffer()
    {
        return millisecondsToBuffer;
    }

    public long getPreviousFlushTimeMillisecond()
    {
        return previousFlushTimeMillisecond;
    }

    public int getTotalBytes()
    {
        return totalBytes;
    }

    public int getTotalRecords()
    {
        return buffer.size() + bulkBuffer.size();
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
        boolean timeThreshold = System.currentTimeMillis() - previousFlushTimeMillisecond >= getMillisecondsToBuffer();
        boolean dataThreshold = totalBytes >= maxBytes;
        return timeThreshold || dataThreshold;
    }

    public Records getRecords()
    {
        return new Records(buffer, bulkBuffer);
    }

    public void consumeRecords(Iterable<T> records)
    {
        for (T record : records) {
            buffer.add(record);

            long size = sizeCalculator.calculate(record);
            totalBytes += size;
        }
    }

    public interface SizeCalculator<T>
    {
        long calculate(T record);
    }

    public class Records
    {
        public final List<T> buffer;
        public final List<T> bulkBuffer;

        public Records(List<T> buffer, List<T> bulkBuffer)
        {
            this.buffer = buffer;
            this.bulkBuffer = bulkBuffer;
        }
    }
}
