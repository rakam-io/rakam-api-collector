/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.spi.SchemaTableName;
import io.airlift.log.Logger;
import io.rakam.presto.deserialization.TableData;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MiddlewareBuffer
{
    private static final Logger log = Logger.get(MiddlewareBuffer.class);
    private final Map<SchemaTableName, List<TableCheckpoint>> batches;
    private final MiddlewareConfig config;
    private final MemoryTracker memoryTracker;
    private Map<SchemaTableName, AtomicLong> bufferRecordCount = new ConcurrentHashMap<>();
    private Map<SchemaTableName, AtomicLong> bufferSize = new ConcurrentHashMap<>();
    private Map<SchemaTableName, Long> bufferLastUpdated = new ConcurrentHashMap<>();

    public MiddlewareBuffer(MiddlewareConfig middlewareConfig, MemoryTracker memoryTracker)
    {
        batches = new ConcurrentHashMap<>();
        this.config = middlewareConfig;
        this.memoryTracker = memoryTracker;
    }

    public void add(BatchRecords records)
    {
        long nowInMillis = System.currentTimeMillis();
        for (Map.Entry<SchemaTableName, TableData> entry : records.getTable().entrySet()) {
            batches.computeIfAbsent(entry.getKey(), (e) ->
                    new ArrayList<>()).add(new TableCheckpoint(records, entry.getKey()));

            bufferRecordCount.computeIfAbsent(entry.getKey(), (e) -> new AtomicLong())
                    .addAndGet(entry.getValue().page.getPositionCount());
            bufferSize.computeIfAbsent(entry.getKey(), (e) -> new AtomicLong())
                    .addAndGet(entry.getValue().page.getRetainedSizeInBytes());
            bufferLastUpdated.compute(entry.getKey(), (tableName, aLong) -> nowInMillis);

            memoryTracker.reserveMemory(entry.getValue().page.getRetainedSizeInBytes());
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

    private boolean shouldFlush(long now, SchemaTableName name)
    {
        Long previousFlushTimeMillisecond = bufferLastUpdated.get(name);
        long timelapseMillisecond = now - previousFlushTimeMillisecond;
        return ((timelapseMillisecond >= config.getMaxFlushDuration().toMillis()));
    }

    public Map<SchemaTableName, List<TableCheckpoint>> getRecordsToBeFlushed()
    {
        long now = System.currentTimeMillis();
        long availableMemory = memoryTracker.availableMemory();
        Map<SchemaTableName, List<TableCheckpoint>> map = new ConcurrentHashMap<>();
        Iterator<Map.Entry<SchemaTableName, List<TableCheckpoint>>> iterator = batches.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<SchemaTableName, List<TableCheckpoint>> entry = iterator.next();
            if (availableMemory == -1 || shouldFlush(now, entry.getKey())) {
                SchemaTableName tableName = entry.getKey();
                log.debug("table_name: " + tableName + " record_count: " + bufferRecordCount.get(tableName).get() + " size: " + bufferSize.get(tableName).get() + " bytes");
                List<TableCheckpoint> value = entry.getValue();
                map.computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).addAll(value);
                iterator.remove();
                bufferRecordCount.remove(tableName);
                bufferSize.remove(tableName);
            }
        }
        return map;
    }
}
