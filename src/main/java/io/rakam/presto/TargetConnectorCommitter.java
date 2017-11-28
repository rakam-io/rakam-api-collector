/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.spi.SchemaTableName;
import com.nurkiewicz.asyncretry.AsyncRetryExecutor;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class TargetConnectorCommitter
{
    private static final Logger log = Logger.get(TargetConnectorCommitter.class);
    private final DatabaseHandler databaseHandler;
    private final AsyncRetryExecutor executor;

    @Inject
    public TargetConnectorCommitter(DatabaseHandler databaseHandler)
    {
        this.databaseHandler = databaseHandler;

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
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
        long size = 0;
        for (MiddlewareBuffer.TableCheckpoint batch : batches) {
            insert.addPage(batch.getTable().page);
            size += batch.getTable().page.getSizeInBytes();
        }
        log.info("Committing " + size + " bytes for table: " + table.getTableName());
        return insert.commit();
    }

    private CompletableFuture<Void> processInternal(SchemaTableName table, List<MiddlewareBuffer.TableCheckpoint> value)
    {
        return commit(value, table).thenRun(() -> checkpoint(value));
    }

    public void process(SchemaTableName table, List<MiddlewareBuffer.TableCheckpoint> value)
    {

        try {
            processInternal(table, value).get();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        catch (ExecutionException e) {
            e.printStackTrace();
        }
        /*executor.getFutureWithRetry(retryContext -> processInternal(table, value)).whenComplete((aVoid, throwable) -> {
            if (throwable != null) {
                log.error(throwable, "Error while processing records");
                // TODO: What should we do if we can't process the data?
                checkpoint(value);
            }
        });*/
    }

    public void checkpoint(List<MiddlewareBuffer.TableCheckpoint> value)
    {
        for (MiddlewareBuffer.TableCheckpoint tableCheckpoint : value) {
            try {
                tableCheckpoint.checkpoint();
            }
            catch (BatchRecords.CheckpointException e) {
                log.error(e, "Error while checkpointing records");
            }
        }
    }

    /*catch (Exception e) {
                        Throwable throwable = e.getCause().getCause();
                        if (throwable!=null && throwable.getClass() != null && throwable.getClass().equals(PrestoException.class)) {
                            throwable = throwable.getCause();
                            if (throwable != null && (throwable.getClass().equals(SdkClientException.class) || throwable.getClass().equals(SocketTimeoutException.class))) {
                                throw new UncheckedIOException(new IOException("Unable to upload data to s3. Check the credentials"));
                            }
                        }
                        log.error(e, "Unable to commit table %s.", table);}*/
}
