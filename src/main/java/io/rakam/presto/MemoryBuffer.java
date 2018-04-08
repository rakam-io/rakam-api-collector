/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

public interface MemoryBuffer<T>
{
    void consumeRecord(T record);

    void consumeBatch(T record, long length);

    boolean shouldFlush();

    void clear();
}
