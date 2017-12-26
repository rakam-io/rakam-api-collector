/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.SchemaTableName;
import com.nurkiewicz.asyncretry.AsyncRetryExecutor;
import io.airlift.log.Logger;
import io.rakam.presto.deserialization.TableData;

import javax.inject.Inject;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class TargetConnectorCommitter {
    private static final long DAY_IN_MILLISECONDS = ChronoUnit.DAYS.getDuration().toMillis() * 2;

    private static final Logger log = Logger.get(TargetConnectorCommitter.class);
    private final DatabaseHandler databaseHandler;
    private final AsyncRetryExecutor executor;
    private final HistoricalDataHandler historicalDataHandler;

    public TargetConnectorCommitter(DatabaseHandler databaseHandler) {
        this(databaseHandler, null);
    }

    @Inject
    public TargetConnectorCommitter(DatabaseHandler databaseHandler, HistoricalDataHandler historicalDataHandler) {
        this.databaseHandler = databaseHandler;

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
            Instant now = Instant.now();
            List<Page> historicalData = new ArrayList<>(batches.size());
            for (MiddlewareBuffer.TableCheckpoint batch : batches) {
                TableData.ExtractedPages pages = batch.getTable().extract(now, DAY_IN_MILLISECONDS);
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
