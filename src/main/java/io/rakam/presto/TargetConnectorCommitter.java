/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.Iterables;
import com.nurkiewicz.asyncretry.AsyncRetryExecutor;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class TargetConnectorCommitter
{
    private final DatabaseHandler databaseHandler;
    private final AsyncRetryExecutor executor;

    @Inject
    public TargetConnectorCommitter(DatabaseHandler databaseHandler)
    {
        this.databaseHandler = databaseHandler;

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        executor = new AsyncRetryExecutor(scheduler).
                firstRetryNoDelay().
                withExponentialBackoff(500, 2).
                withMaxDelay(10_000).
                withUniformJitter().
                withMaxRetries(5);
    }

    private CompletableFuture<Void> commit(List<MiddlewareBuffer.TableCheckpoint> batches, SchemaTableName table)
    {
        DatabaseHandler.Inserter insert = databaseHandler.insert(table.getSchemaName(), table.getTableName());

        for (MiddlewareBuffer.TableCheckpoint batch : batches) {
            insert.addPage(batch.getTable().page);
        }

        return insert.commit();
    }

    private CompletableFuture<Void> processInternal(SchemaTableName table, List<MiddlewareBuffer.TableCheckpoint> value)
    {
        return commit(value, table);
    }

    public CompletableFuture<Void> process(SchemaTableName table, List<MiddlewareBuffer.TableCheckpoint> value)
    {
        return this.executor.getFutureWithRetry(retryContext -> processInternal(table, value));
    }
}
