/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import io.airlift.configuration.Config;
import org.weakref.jmx.Managed;

import javax.validation.constraints.Max;

import java.util.concurrent.atomic.AtomicLong;

public class MemoryTracker
{
    public static final long HEAP_MAX_SIZE = Runtime.getRuntime().freeMemory();
    private final double availableRatio;
    private final long availableHeapSize;

    private AtomicLong reservedMemory;

    public MemoryTracker(MemoryConfig memoryConfig)
    {
        reservedMemory = new AtomicLong();
        availableRatio = memoryConfig.getHeapRatio();
        availableHeapSize = (long) (HEAP_MAX_SIZE * availableRatio);
    }

    @Managed
    public long getAvailableHeapSize()
    {
        return availableHeapSize;
    }

    public void reserveMemory(long bytes)
    {
        reservedMemory.addAndGet(bytes);
    }

    @Managed
    public long availableMemory()
    {
        return availableHeapSize - reservedMemory.get();
    }

    @Managed
    public double availableMemoryInPercentage()
    {
        return availableMemory() * 1.0 / availableHeapSize;
    }

    public void freeMemory(long bytes)
    {
        if(bytes < -1) {
            throw new IllegalArgumentException();
        }
        reservedMemory.addAndGet(-bytes);
    }

    public static class MemoryConfig
    {
        private double heapRatio = .7;

        @Max(1)
        public double getHeapRatio()
        {
            return heapRatio;
        }

        @Config("memory.heap-ratio")
        public void setHeapRatio(double heapRatio)
        {
            if (heapRatio > +1) {
                throw new IllegalStateException("memory.heap-ratio must be a value between 0 and 1");
            }
            this.heapRatio = heapRatio;
        }
    }
}
