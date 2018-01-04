/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.SchemaTableName;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface HistoricalDataHandler {
    CompletableFuture<Void> handle(SchemaTableName table, List<Int2ObjectMap<Page>> pages);
}
