/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import org.weakref.jmx.Managed;

import java.util.concurrent.atomic.AtomicLong;

public class MemoryTracker
{
    public static final long HEAP_MAX_SIZE = (long) (Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory());
    // 0.1 is available for memory buffer
    private static final double AVAILABLE_RATIO = .7;
    private static final long AVAILABLE_HEAP_SIZE = (long) (HEAP_MAX_SIZE * AVAILABLE_RATIO);

    private AtomicLong reservedMemory;

    public MemoryTracker()
    {
        reservedMemory = new AtomicLong();
    }

    @Managed
    public static long getAvailableHeapSize()
    {
        return AVAILABLE_HEAP_SIZE;
    }

    public void reserveMemory(long bytes)
    {
        reservedMemory.addAndGet(bytes);
    }

    @Managed
    public long availableMemory()
    {
        return AVAILABLE_HEAP_SIZE - reservedMemory.get();
    }

    @Managed
    public double availableMemoryInPercentage()
    {
        return availableMemory() * 1.0 / AVAILABLE_HEAP_SIZE;
    }

    public void freeMemory(long bytes)
    {
        reservedMemory.addAndGet(-bytes);
    }
}
