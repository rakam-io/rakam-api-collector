/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.SchemaTableName;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface HistoricalDataHandler {
    CompletableFuture<Void> handle(SchemaTableName table, List<Page> pages);
}
