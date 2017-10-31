/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.Table;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.rakam.presto.deserialization.TableData;

import javax.inject.Inject;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

public class TargetConnectorCommitter
{
    private static final Logger log = Logger.get(TargetConnectorCommitter.class);
    private final DatabaseHandler databaseHandler;

    @Inject
    public TargetConnectorCommitter(DatabaseHandler databaseHandler)
    {
        this.databaseHandler = databaseHandler;
    }

    public void process(Iterable<Table<String, String, TableData>> batches)
    {
        StreamSupport.stream(batches.spliterator(), false).flatMap(t -> t.cellSet().stream()
                .map(b -> new SchemaTableName(b.getRowKey(), b.getColumnKey()))).distinct().forEach(table -> {

            try {
                long startTime = System.currentTimeMillis();
                RetryDriver.retry().maxAttempts(5)
                        .stopOn(InterruptedException.class)
                        .exponentialBackoff(
                                new Duration(1, TimeUnit.SECONDS),
                                new Duration(1, TimeUnit.MINUTES),
                                new Duration(1, TimeUnit.MILLISECONDS), 2.0)
                        .onRetry(() -> log.warn("Retrying to save data"))
                        .run("middlewareConnector", () -> commit(batches, table).join());
                long endTime = System.currentTimeMillis();

                log.info("commit execution time: " + (endTime - startTime) + " for table: " + table.getTableName());
            }
            catch (Exception e) {
                e.printStackTrace();
                log.error(e, "Unable to commit table %s.", table);
            }
        });
    }

    private CompletableFuture<Void> commit(Iterable<Table<String, String, TableData>> batches, SchemaTableName table)
    {
        DatabaseHandler.Inserter insert = databaseHandler.insert(table.getSchemaName(), table.getTableName());
        long size = 0;
        for (Table<String, String, TableData> batch : batches) {
            TableData tableData = batch.get(table.getSchemaName(), table.getTableName());
            if (tableData != null) {
                size += tableData.page.getSizeInBytes();
                insert.addPage(tableData.page);
            }
        }
        log.info("size in Bytes: " + (size));
        return insert.commit();
    }
}
