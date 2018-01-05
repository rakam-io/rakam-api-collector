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
    private final ArrayList<T> pageBuffer;
    private final SizeCalculator<T> sizeCalculator;
    private final MemoryTracker memoryTracker;
    private long previousFlushTimeMillisecond;
    private int totalBytes;

    public BasicMemoryBuffer(StreamConfig config, MemoryTracker memoryTracker, SizeCalculator<T> sizeCalculator)
    {
        this.sizeCalculator = sizeCalculator;
        this.memoryTracker = memoryTracker;
        millisecondsToBuffer = config.getMaxFlushDuration().toMillis();
        previousFlushTimeMillisecond = System.currentTimeMillis();
        buffer = new ArrayList<>(1000);
        bulkBuffer = new ArrayList<>(1000);
        pageBuffer = new ArrayList<>(1000);
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

    public void consumeRecord(T record, long size)
    {
        buffer.add(record);
        totalBytes += size;
        memoryTracker.reserveMemory(size);
    }

    public void consumeBatch(T record, long size)
    {
        memoryTracker.reserveMemory(size);
        bulkBuffer.add(record);
        totalBytes += size;
    }

    public void consumePage(T record, long size)
    {
        pageBuffer.add(record);
        totalBytes += size;
        memoryTracker.reserveMemory(size);
    }

    public void clear()
    {
        buffer.clear();
        bulkBuffer.clear();
        pageBuffer.clear();
        previousFlushTimeMillisecond = System.currentTimeMillis();
        memoryTracker.freeMemory(totalBytes);
        totalBytes = 0;
    }

    public boolean shouldFlush()
    {
        long timelapseMillisecond = System.currentTimeMillis() - previousFlushTimeMillisecond;
        return (timelapseMillisecond >= getMillisecondsToBuffer());
    }

    public Records getRecords()
    {
        return new Records(buffer, bulkBuffer, pageBuffer);
    }

    public void consumeRecords(Iterable<T> records)
    {
        long initialSize = totalBytes;
        for (T record : records) {
            buffer.add(record);

            long size = sizeCalculator.calculate(record);
            totalBytes += size;
        }

        memoryTracker.reserveMemory(totalBytes - initialSize);
    }

    public class Records
    {
        public final List<T> buffer;
        public final List<T> bulkBuffer;
        public final List<T> pageBuffer;

        public Records(List<T> buffer, List<T> bulkBuffer, List<T> pageBuffer)
        {
            this.buffer = buffer;
            this.bulkBuffer = bulkBuffer;
            this.pageBuffer = pageBuffer;
        }
    }

    public interface SizeCalculator<T>
    {
        long calculate(T record);
    }
}
