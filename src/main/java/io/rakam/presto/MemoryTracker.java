package io.rakam.presto;

import java.util.concurrent.atomic.AtomicLong;

public class MemoryTracker {
    private static final double heapMaxSize = Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory();
    private double dynamicMemoryRatio = .3;

    private AtomicLong reservedMemory;

    public MemoryTracker() {
        reservedMemory = new AtomicLong();
    }

    public void reserveMemory(long bytes) {
        reservedMemory.addAndGet(bytes);
    }

    public long availableMemory() {
        double availableMemory = heapMaxSize - reservedMemory.get();
        double availableRatio = availableMemory / heapMaxSize;
        if (availableRatio > dynamicMemoryRatio) {
            return (long) ((availableRatio - dynamicMemoryRatio) * heapMaxSize);
        } else {
            return -1;
        }
    }

    public void freeMemory(long bytes) {
        reservedMemory.addAndGet(-bytes);
    }
}
