/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Page;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface DatabaseHandler
{
    List<ColumnMetadata> getColumns(String schema, String table);

    List<ColumnMetadata> addColumns(String schema, String table, List<ColumnMetadata> columns);

    Inserter insert(String schema, String table);

    interface Inserter {

        void addPage(Page page);

        CompletableFuture<Void> commit();
    }
}
