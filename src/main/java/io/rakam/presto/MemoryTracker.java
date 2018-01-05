/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import java.util.concurrent.atomic.AtomicLong;

public class MemoryTracker
{
    private static final long HEAP_MAX_SIZE = (long) (Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory());
    private static final double AVAILABLE_RATIO = .3;
    private static final long AVAILABLE_HEAP_SIZE = (long) (HEAP_MAX_SIZE * AVAILABLE_RATIO);

    private AtomicLong reservedMemory;

    public MemoryTracker()
    {
        reservedMemory = new AtomicLong();
    }

    public void reserveMemory(long bytes)
    {
        reservedMemory.addAndGet(bytes);
    }

    public long availableMemory()
    {
        return AVAILABLE_HEAP_SIZE - reservedMemory.get();
    }

    public void freeMemory(long bytes)
    {
        reservedMemory.addAndGet(-bytes);
    }
}
