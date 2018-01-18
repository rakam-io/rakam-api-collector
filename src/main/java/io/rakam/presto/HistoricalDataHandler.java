/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import java.util.concurrent.CompletableFuture;

public interface HistoricalDataHandler<T>
{
    CompletableFuture<Void> handle(Iterable<T> table, int recordCount);
}
