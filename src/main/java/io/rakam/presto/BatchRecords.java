/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.google.common.collect.Table;

public class BatchRecords
{
    private final Table<String, String, MessageEventTransformer.TableData> table;
    private final Checkpointer checkpointer;

    public BatchRecords(Table<String, String, MessageEventTransformer.TableData> table, Checkpointer checkpointer)
    {
        this.table = table;
        this.checkpointer = checkpointer;
    }

    public Table<String, String, MessageEventTransformer.TableData> getTable()
    {
        return table;
    }

    public void checkpoint()
            throws CheckpointException
    {
        checkpointer.checkpoint();
    }

    public interface Checkpointer
    {
        void checkpoint() throws CheckpointException;
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
