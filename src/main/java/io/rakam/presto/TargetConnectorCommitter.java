/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.google.common.collect.Table;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.util.Arrays;
import java.util.List;
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

    public void process(Iterable<Table<String, String, MessageEventTransformer.TableData>> batches)
    {
        StreamSupport.stream(batches.spliterator(), false).flatMap(t -> t.cellSet().stream()
                .map(b -> new SchemaTableName(b.getRowKey(), b.getColumnKey()))).distinct().forEach(table -> {

            try {
                RetryDriver.retry().maxAttempts(5)
                        .stopOn(InterruptedException.class)
                        .exponentialBackoff(
                                new Duration(1, TimeUnit.SECONDS),
                                new Duration(1, TimeUnit.MINUTES),
                                new Duration(1, TimeUnit.MILLISECONDS), 2.0)
                        .onRetry(() -> log.warn("Retrying to save data"))
                        .run("middlewareConnector", () -> commit(batches, table).join());
            }
            catch (Exception e) {
                log.error(e, "Unable to commit table %s.", table);
            }
        });
    }

    private CompletableFuture<Void> commit(Iterable<Table<String, String, MessageEventTransformer.TableData>> batches, SchemaTableName table)
    {
        List<ColumnMetadata> columns = databaseHandler.getColumns(table.getSchemaName(), table.getTableName());

        DatabaseHandler.Inserter insert = databaseHandler.insert(table.getSchemaName(), table.getTableName());

        for (Table<String, String, MessageEventTransformer.TableData> batch : batches) {
            MessageEventTransformer.TableData tableData = batch.get(table.getSchemaName(), table.getTableName());
            if (tableData != null) {
                Page page = tableData.page;
                if (columns.size() != page.getChannelCount()) {
                    Block[] blocks = Arrays.copyOf(page.getBlocks(), columns.size());
                    for (int i = page.getChannelCount(); i < columns.size(); i++) {
                        BlockBuilder blockBuilder = columns.get(i).getType().createBlockBuilder(new BlockBuilderStatus(), page.getPositionCount());
                        for (int i1 = 0; i1 < page.getPositionCount(); i1++) {
                            blockBuilder.appendNull();
                        }
                        blocks[i] = blockBuilder.build();
                        page = new Page(blocks);
                    }

                    page = new Page(blocks);
                }

                insert.addPage(page);
            }
        }

        return insert.commit();
    }
}
