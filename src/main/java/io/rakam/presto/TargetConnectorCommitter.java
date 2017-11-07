/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.spi.SchemaTableName;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class TargetConnectorCommitter {
    private static final Logger log = Logger.get(TargetConnectorCommitter.class);
    private final DatabaseHandler databaseHandler;

    @Inject
    public TargetConnectorCommitter(DatabaseHandler databaseHandler) {
        this.databaseHandler = databaseHandler;
    }

    private CompletableFuture<Void> commit(List<MiddlewareBuffer.TableCheckpoint> batches, SchemaTableName table) {
        DatabaseHandler.Inserter insert = databaseHandler.insert(table.getSchemaName(), table.getTableName());

        for (MiddlewareBuffer.TableCheckpoint batch : batches) {
            insert.addPage(batch.getTable().page);
        }

        return insert.commit();
    }

    public void process(SchemaTableName table, List<MiddlewareBuffer.TableCheckpoint> value) {
        try {
            RetryDriver.retry().maxAttempts(5)
                    .stopOn(InterruptedException.class)
                    .exponentialBackoff(
                            new Duration(1, TimeUnit.SECONDS),
                            new Duration(1, TimeUnit.MINUTES),
                            new Duration(1, TimeUnit.MILLISECONDS), 2.0)
                    .onRetry(() -> log.warn("Retrying to save data"))
                    .run("middlewareConnector", () -> commit(value, table).join());
        } catch (Exception e) {
            log.error(e, "Unable to commit table %s.", table);
        }

        for (MiddlewareBuffer.TableCheckpoint tableCheckpoint : value) {
            try {
                tableCheckpoint.checkpoint();
            } catch (BatchRecords.CheckpointException e) {
                log.error(e, "Error while checkpointing records");
            }
        }
    }
}
