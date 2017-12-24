/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.spi.SchemaTableName;
import com.nurkiewicz.asyncretry.AsyncRetryExecutor;
import io.airlift.log.Logger;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.*;

public class TargetConnectorCommitter {
    private static final Logger log = Logger.get(TargetConnectorCommitter.class);
    private final DatabaseHandler databaseHandler;
    private final AsyncRetryExecutor executor;
    private final Executor thisThread;

    @Inject
    public TargetConnectorCommitter(DatabaseHandler databaseHandler) {
        this.databaseHandler = databaseHandler;

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        executor = new AsyncRetryExecutor(scheduler).
                firstRetryNoDelay().
                withExponentialBackoff(500, 2).
                withMaxDelay(10_000).
                withUniformJitter().
                withMaxRetries(5);
        thisThread = new ThisThreadExecutor();
    }

    private CompletableFuture<Void> commit(List<MiddlewareBuffer.TableCheckpoint> batches, SchemaTableName table) {
        DatabaseHandler.Inserter insert = databaseHandler.insert(table.getSchemaName(), table.getTableName());

        for (MiddlewareBuffer.TableCheckpoint batch : batches) {
            insert.addPage(batch.getTable().page);
        }

        return insert.commit();
    }

    private CompletableFuture<Void> processInternal(SchemaTableName table, List<MiddlewareBuffer.TableCheckpoint> value) {
        return commit(value, table);
    }

    public CompletableFuture<Void> process(SchemaTableName table, List<MiddlewareBuffer.TableCheckpoint> value) {
        return this.executor.getFutureWithRetry(retryContext -> processInternal(table, value));
    }

    private static class ThisThreadExecutor implements Executor {
        @Override
        public void execute(Runnable command) {
            command.run();
        }
    }
}
