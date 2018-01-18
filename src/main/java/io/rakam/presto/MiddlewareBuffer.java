/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.rakam.presto.deserialization.TableData;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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

        for (Map.Entry<SchemaTableName, TableData> entry : records.getTable().entrySet()) {
            SchemaTableName tableName = entry.getKey();
            batches.computeIfAbsent(tableName, (e) ->
                    new ArrayList<>()).add(new TableCheckpoint(records, tableName));

            bufferRecordCount.computeIfAbsent(tableName, (e) -> new Counter())
                    .increment(entry.getValue().page.getPositionCount());
            bufferSize.computeIfAbsent(tableName, (e) -> new Counter())
                    .increment(entry.getValue().page.getRetainedSizeInBytes());

            memoryTracker.reserveMemory(entry.getValue().page.getRetainedSizeInBytes());
            bufferLastFlushed.putIfAbsent(tableName, now);
        }
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
        long memoryNeedsToBeAvailable = (long) (MemoryTracker.getAvailableHeapSize() * .6);
        long availableMemory = memoryTracker.availableMemory();

        Map<SchemaTableName, List<TableCheckpoint>> map = new ConcurrentHashMap<>();
        Iterator<Map.Entry<SchemaTableName, List<TableCheckpoint>>> iterator = batches.entrySet().iterator();

        Set<SchemaTableName> tablesToBeFlushed;
        if (memoryNeedsToBeAvailable > availableMemory) {
            List<Map.Entry<SchemaTableName, Counter>> sortedTables = bufferSize.entrySet().stream()
                    .sorted(Comparator.comparingLong(o -> -o.getValue().value))
                    .collect(Collectors.toList());

            tablesToBeFlushed = new HashSet<>();
            for (Map.Entry<SchemaTableName, Counter> table : sortedTables) {
                if (memoryNeedsToBeAvailable < availableMemory) {
                    break;
                }

                tablesToBeFlushed.add(table.getKey());
                availableMemory += table.getValue().value;
            }
        }
        else {
            tablesToBeFlushed = ImmutableSet.of();
        }

        while (iterator.hasNext()) {
            Map.Entry<SchemaTableName, List<TableCheckpoint>> entry = iterator.next();
            SchemaTableName tableName = entry.getKey();
            List<TableCheckpoint> value = entry.getValue();

            if (shouldFlush(now, tableName) || tablesToBeFlushed.contains(tableName)) {
                map.computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).addAll(value);
                iterator.remove();
                bufferLastFlushed.put(tableName, now);
                bufferRecordCount.remove(tableName);
                bufferSize.remove(tableName);
            }
        }
        return map;
    }

    private static class Counter
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

        public void checkpoint()
                throws BatchRecords.CheckpointException
        {
            batchRecords.checkpoint(tableName);
        }

        public TableData getTable()
        {
            return batchRecords.getTable().get(tableName);
        }
    }
}
