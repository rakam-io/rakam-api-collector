/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.TimestampType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class TestDatabaseHandler
        implements DatabaseHandler {

    protected final Map<String, Map<String, List<ColumnMetadata>>> columns;
    private final boolean flexibleSchema;

    public TestDatabaseHandler()
    {
        this.columns = new HashMap<>();
        this.flexibleSchema = true;
    }

    public TestDatabaseHandler(String schema, String table, List<ColumnMetadata> columns)
    {
        this(schema, table, columns, false);
    }

    public TestDatabaseHandler(String schemaName, String tableName, List<ColumnMetadata> columns, boolean flexibleSchema)
    {
        this.columns = new HashMap<>();
        HashMap<String, List<ColumnMetadata>> table = new HashMap<>();
        table.put(tableName, new ArrayList<>(columns));

        this.columns.put(schemaName, table);
        this.flexibleSchema = flexibleSchema;
    }

    @Override
    public List<ColumnMetadata> getColumns(String schema, String table)
    {
        Map<String, List<ColumnMetadata>> map = this.columns.get(schema);
        if(map != null) {
            List<ColumnMetadata> columnList = map.get(table);
            if(columnList != null) {
                return columnList;
            }
        }

        throw new IllegalArgumentException();
    }

    @Override
    public synchronized List<ColumnMetadata> addColumns(String schema, String table, List<ColumnMetadata> columns)
    {
        if(!flexibleSchema) {
            throw new UnsupportedOperationException();
        }

        Map<String, List<ColumnMetadata>> tables = this.columns.get(schema);
        if(tables == null) {
            tables = new HashMap<>();
            this.columns.put(schema, tables);
        }

        List<ColumnMetadata> columnList = tables.get(table);

        if(columnList == null) {
            columnList = new ArrayList<>();
            columnList.add(new ColumnMetadata("_shard_time", TimestampType.TIMESTAMP));
            tables.put(table, columnList);
        }

        List<ColumnMetadata> finalColumnList = columnList;
        columns.stream()
                .filter(column -> !finalColumnList.stream().map(e -> e.getName()).anyMatch(name -> name.equals(column.getName())))
                .forEach(columnList::add);

        return finalColumnList;
    }

    @Override
    public Inserter insert(String schema, String table)
    {
        boolean[] isDone = new boolean[1];

        return new Inserter() {
            @Override
            public void addPage(Page page)
            {
                isDone[0] = true;
            }

            @Override
            public CompletableFuture<Void> commit()
            {
                if(!isDone[0]) {
                    throw new IllegalStateException();
                }
                return CompletableFuture.completedFuture(null);
            }
        };
    }
}
