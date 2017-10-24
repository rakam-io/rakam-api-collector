/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import com.facebook.presto.spi.SchemaTableName;
import io.rakam.presto.MemoryBuffer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaMemoryBuffer<T>
{
    private final Map<SchemaTableName, Buffer<T>> partitionedBuffer;
    private final int numMessagesToBuffer;
    private final long millisecondsToBuffer;
    private long previousFlushTimeMillisecond;

    public KafkaMemoryBuffer()
    {
        this.numMessagesToBuffer = 100000;
        this.millisecondsToBuffer = 15000;
        this.previousFlushTimeMillisecond = getCurrentTimeMilliseconds();
        this.partitionedBuffer = new ConcurrentHashMap<>();
    }

    public synchronized Buffer<T> createOrGetPartitionedBuffer(SchemaTableName collection)
    {

        Buffer<T> existingBuffer = partitionedBuffer.get(collection);
        if (existingBuffer != null) {
            throw new IllegalStateException();
        }

        Buffer<T> tPartitionedMemoryBuffer = new Buffer<>(numMessagesToBuffer / 2);
        partitionedBuffer.put(collection, tPartitionedMemoryBuffer);

        return tPartitionedMemoryBuffer;
    }

    public int getPartitionCount()
    {
        return partitionedBuffer.size();
    }

    public boolean shouldFlush()
    {
        long timelapseMillisecond = getCurrentTimeMilliseconds() - previousFlushTimeMillisecond;
        int size = 0;
        for (Map.Entry<SchemaTableName, Buffer<T>> entry : partitionedBuffer.entrySet()) {
            size += entry.getValue().size();
        }
        return ((size >= getNumRecordsToBuffer()) || (timelapseMillisecond >= getMillisecondsToBuffer()));
    }

    protected long getCurrentTimeMilliseconds()
    {
        return System.currentTimeMillis();
    }

    public long getNumRecordsToBuffer()
    {
        return numMessagesToBuffer;
    }

    public long getMillisecondsToBuffer()
    {
        return millisecondsToBuffer;
    }

    public void flush()
    {
        previousFlushTimeMillisecond = getCurrentTimeMilliseconds();
    }

    public class Buffer<T>
            implements MemoryBuffer<T>
    {
        private final List<T> objects;
        private final List<T> bulkObjects;

        public Buffer(int initialNumberOfItem)
        {
            this.objects = new ArrayList<>(initialNumberOfItem);
            this.bulkObjects = new ArrayList<>(initialNumberOfItem);
        }

        public List<T> getRecords()
        {
            return objects;
        }

        @Override
        public void consumeRecord(T record, long length)
        {
            objects.add(record);
        }

        @Override
        public void consumeBatch(T record, long length)
        {
            bulkObjects.add(record);
        }

        @Override
        public boolean shouldFlush()
        {
            return KafkaMemoryBuffer.this.shouldFlush();
        }

        public int size()
        {
            return objects.size();
        }

        @Override
        public void clear()
        {
            objects.clear();
        }
    }
}
