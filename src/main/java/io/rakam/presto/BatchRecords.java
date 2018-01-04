/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.spi.SchemaTableName;
import io.rakam.presto.deserialization.TableData;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class BatchRecords
{
    private final Map<SchemaTableName, TableData> table;
    private final Set<SchemaTableName> committed;
    private final Checkpointer checkpointer;

    public BatchRecords(Map<SchemaTableName, TableData> table, Checkpointer checkpointer)
    {
        this.table = table;
        this.committed = new HashSet<>();
        this.checkpointer = checkpointer;
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
            checkpointer.checkpoint();
        }
    }

    public interface Checkpointer
    {
        void checkpoint()
                throws CheckpointException;
    }

    public static class CheckpointException
            extends Exception
    {
        public CheckpointException(Throwable cause)
        {
            super("Error while performing checkpoint operation.", cause);
        }
    }
}
