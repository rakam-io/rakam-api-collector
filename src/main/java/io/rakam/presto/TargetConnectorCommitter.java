/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import net.jodah.failsafe.AsyncFailsafe;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TargetConnectorCommitter
{
    private final int executorPoolSize;
    private AtomicInteger activeFlushCount = new AtomicInteger();
    private final DatabaseHandler databaseHandler;
    private final AsyncFailsafe<Void> executor;

    @Inject
    public TargetConnectorCommitter(DatabaseHandler databaseHandler)
    {
        this.databaseHandler = databaseHandler;

        RetryPolicy retryPolicy = new RetryPolicy()
                .withBackoff(1, 60, TimeUnit.SECONDS)
                .withJitter(.1)
                .withMaxDuration(1, TimeUnit.MINUTES)
                .withMaxRetries(3);

        executorPoolSize = Runtime.getRuntime().availableProcessors() * 2;
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(executorPoolSize,
                new ThreadFactoryBuilder().setNameFormat("target-committer").build());

        executor = Failsafe.<Void>with(retryPolicy).with(scheduler);
    }

    private CompletableFuture<Void> commit(List<MiddlewareBuffer.TableCheckpoint> batches, SchemaTableName table)
    {
        DatabaseHandler.Inserter insert = databaseHandler.insert(table.getSchemaName(), table.getTableName());

        for (MiddlewareBuffer.TableCheckpoint batch : batches) {
            insert.addPage(batch.getTable().page);
        }

        return insert.commit();
    }

    public boolean isFull()
    {
        return activeFlushCount.get() / executorPoolSize > 3;
    }

    public int getActiveFlushCount()
    {
        return activeFlushCount.get();
    }

    private CompletableFuture<Void> processInternal(SchemaTableName table, List<MiddlewareBuffer.TableCheckpoint> value)
    {
        return commit(value, table);
    }

    public CompletableFuture<Void> process(SchemaTableName table, List<MiddlewareBuffer.TableCheckpoint> value)
    {
        activeFlushCount.incrementAndGet();
        CompletableFuture<Void> future = executor.future(() -> processInternal(table, value));
        future.whenComplete((aVoid, throwable) -> activeFlushCount.decrementAndGet());
        return future;
    }
}
