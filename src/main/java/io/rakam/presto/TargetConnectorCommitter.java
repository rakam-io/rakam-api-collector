/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.SchemaTableName;
import com.nurkiewicz.asyncretry.AsyncRetryExecutor;
import io.airlift.log.Logger;
import io.rakam.presto.deserialization.TableData;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import jdk.nashorn.internal.ir.Block;

import javax.inject.Inject;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class TargetConnectorCommitter {
    private final DatabaseHandler databaseHandler;
    private final AsyncRetryExecutor executor;
    private final HistoricalDataHandler historicalDataHandler;
    private final MemoryTracker memoryTracker;

    public TargetConnectorCommitter(DatabaseHandler databaseHandler, MemoryTracker memoryTracker) {
        this(databaseHandler, memoryTracker, null);
    }

    @Inject
    public TargetConnectorCommitter(DatabaseHandler databaseHandler, MemoryTracker memoryTracker, HistoricalDataHandler historicalDataHandler) {
        this.databaseHandler = databaseHandler;
        this.memoryTracker = memoryTracker;

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        this.historicalDataHandler = historicalDataHandler;
        executor = new AsyncRetryExecutor(scheduler).
                firstRetryNoDelay().
                withExponentialBackoff(500, 2).
                withMaxDelay(10_000).
                withUniformJitter().
                withMaxRetries(5);
    }

    private CompletableFuture<Void> commit(List<MiddlewareBuffer.TableCheckpoint> batches, SchemaTableName table) {
        DatabaseHandler.Inserter insert = databaseHandler.insert(table.getSchemaName(), table.getTableName());

        if (historicalDataHandler != null) {
            LocalDate today = LocalDate.now();
            List<Int2ObjectMap<Page>> historicalData = new ArrayList<>(batches.size());

            for (MiddlewareBuffer.TableCheckpoint batch : batches) {
                TableData.ExtractedPages pages = batch.getTable().extract(today, memoryTracker);
                insert.addPage(pages.now);
                historicalData.add(pages.prev);
            }

            CompletableFuture<Void> historicalDataJob = historicalDataHandler.handle(table, historicalData);
            CompletableFuture<Void> shardWriter = insert.commit();
            return CompletableFuture.allOf(historicalDataJob, shardWriter);
        } else {
            for (MiddlewareBuffer.TableCheckpoint batch : batches) {
                insert.addPage(batch.getTable().page);
            }
            return insert.commit();
        }
    }

    private CompletableFuture<Void> processInternal(SchemaTableName table, List<MiddlewareBuffer.TableCheckpoint> value) {
        return commit(value, table);
    }

    public CompletableFuture<Void> process(SchemaTableName table, List<MiddlewareBuffer.TableCheckpoint> value) {
        return this.executor.getFutureWithRetry(retryContext -> processInternal(table, value));
    }
}
