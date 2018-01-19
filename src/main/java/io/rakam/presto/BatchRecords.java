/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.spi.SchemaTableName;
import io.airlift.log.Logger;
import io.rakam.presto.deserialization.TableData;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class BatchRecords
{
    public static final CompletableFuture COMPLETED_FUTURE = CompletableFuture.completedFuture(1);

    private static final Logger log = Logger.get(BatchRecords.class);

    private final Map<SchemaTableName, TableData> table;

    private final Checkpointer checkpointer;
    private final Set<SchemaTableName> committed;
    private final CompletableFuture<Void> historicalDataHandler;

    public BatchRecords(Map<SchemaTableName, TableData> table, Checkpointer checkpointer)
    {
        this(table, COMPLETED_FUTURE, checkpointer);
    }

    public BatchRecords(Map<SchemaTableName, TableData> table, CompletableFuture historicalDataHandler, Checkpointer checkpointer)
    {
        this.table = table;
        this.committed = new HashSet<>();
        this.checkpointer = checkpointer;
        this.historicalDataHandler = historicalDataHandler;
    }

    public Map<SchemaTableName, TableData> getTable()
    {
        return table;
    }

    public void checkpoint(SchemaTableName tableName)
            throws CheckpointException
    {
        committed.add(tableName);
        if (committed.size() == table.size()) {
            if (historicalDataHandler.isDone()) {
                checkpointer.checkpoint();
            }
            else {
                historicalDataHandler.whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        log.error(throwable, "Error sending historical records");
                    }

                    try {
                        checkpointer.checkpoint();
                    }
                    catch (CheckpointException e) {
                        e.printStackTrace();
                    }
                });
            }
        }
    }

    public interface Checkpointer
    {
        void checkpoint()
                throws CheckpointException;
    }

    public static class CheckpointException
            extends Exception {
        public CheckpointException(Throwable cause) {
            super("Error while performing checkpoint operation.", cause);
        }
    }
}
