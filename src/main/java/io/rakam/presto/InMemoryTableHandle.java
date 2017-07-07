/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;

public class InMemoryTableHandle
        implements ConnectorTableHandle
{
    private final SchemaTableName tableName;

    public InMemoryTableHandle(SchemaTableName tableName) {
        this.tableName = tableName;
    }

    public SchemaTableName getTableName()
    {
        return tableName;
    }
}
