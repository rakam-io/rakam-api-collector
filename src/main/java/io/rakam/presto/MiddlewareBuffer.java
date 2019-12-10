/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.spi.SchemaTableName;
import io.airlift.units.DataSize;
import io.rakam.presto.deserialization.TableData;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MiddlewareBuffer
{
    private final Map<SchemaTableName, List<TableCheckpoint>> batches;
    private final MiddlewareConfig config;
    private final MemoryTracker memoryTracker;
    private Map<SchemaTableName, Counter> bufferRecordCount = new ConcurrentHashMap<>();
    private Map<SchemaTableName, Counter> bufferSize = new ConcurrentHashMap<>();
    private Map<SchemaTableName, Long> bufferLastFlushed = new ConcurrentHashMap<>();

    public MiddlewareBuffer(MiddlewareConfig middlewareConfig, MemoryTracker memoryTracker)
    {
        batches = new ConcurrentHashMap<>();
        this.config = middlewareConfig;
        this.memoryTracker = memoryTracker;
    }

    public void add(BatchRecords records)
    {
        long now = System.currentTimeMillis();

        long totalAllocate = 0;
        for (Map.Entry<SchemaTableName, TableData> entry : records.getTable().entrySet()) {
            SchemaTableName tableName = entry.getKey();
            batches.computeIfAbsent(tableName, (e) ->
                    new ArrayList<>()).add(new TableCheckpoint(records, tableName));

            bufferRecordCount.computeIfAbsent(tableName, (e) -> new Counter())
                    .increment(entry.getValue().page.getPositionCount());
            bufferSize.computeIfAbsent(tableName, (e) -> new Counter())
                    .increment(entry.getValue().page.getRetainedSizeInBytes());

            totalAllocate += entry.getValue().page.getRetainedSizeInBytes();
            bufferLastFlushed.putIfAbsent(tableName, now);
        }

        memoryTracker.reserveMemory(totalAllocate);
    }

    public DataSize calculateSize()
    {
        return DataSize.succinctBytes(batches.values().stream()
                .mapToLong(t -> t.stream().mapToLong(e -> e.getTable().page.getRetainedSizeInBytes()).sum())
                .sum());
    }

    private boolean shouldFlush(long now, SchemaTableName name)
    {
        Long previousFlushTimeMillisecond = bufferLastFlushed.get(name);
        long timeLapseMillisecond = now - previousFlushTimeMillisecond;
        return (timeLapseMillisecond >= config.getMaxFlushDuration().toMillis());
    }

    public Map<SchemaTableName, List<TableCheckpoint>> getRecordsToBeFlushed()
    {
        long now = System.currentTimeMillis();
        long memoryNeedsToBeAvailable = (long) (memoryTracker.getAvailableHeapSize() * .5);
        long availableMemory = memoryTracker.availableMemory();

        Map<SchemaTableName, List<TableCheckpoint>> map = new ConcurrentHashMap<>();

        Iterator<Map.Entry<SchemaTableName, Counter>> iterator = bufferSize.entrySet().stream()
                .sorted(Comparator.comparingLong(o -> -o.getValue().value))
                .iterator();

        while (iterator.hasNext()) {
            Map.Entry<SchemaTableName, Counter> next = iterator.next();
            SchemaTableName tableName = next.getKey();


            boolean systemNeedsMemory = memoryNeedsToBeAvailable > availableMemory;

            if (systemNeedsMemory || shouldFlush(now, tableName)) {

                List<TableCheckpoint> value = batches.remove(tableName);
                if (value == null) {
                    continue;
                }

                map.computeIfAbsent(tableName, k -> new ArrayList<>()).addAll(value);
                bufferLastFlushed.put(tableName, now);
                bufferRecordCount.remove(tableName);
                bufferSize.remove(tableName);
                availableMemory += next.getValue().value;
            }
        }
        return map;
    }

    public static class Counter
    {
        long value;

        public void increment(long incrementBy)
        {
            this.value += incrementBy;
        }
    }

    public static class TableCheckpoint
    {
        private final BatchRecords batchRecords;
        private final SchemaTableName tableName;

        public TableCheckpoint(BatchRecords batchRecords, SchemaTableName tableName)
        {
            this.batchRecords = batchRecords;
            this.tableName = tableName;
        }

        public void checkpoint() throws BatchRecords.CheckpointException
        {
            batchRecords.checkpoint(tableName);
        }

        public TableData getTable()
        {
            return batchRecords.getTable().get(tableName);
        }
    }
}
