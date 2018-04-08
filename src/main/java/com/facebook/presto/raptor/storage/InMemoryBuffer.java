/*
 * Licensed under the Rakam Incorporation
 */

package com.facebook.presto.raptor.storage;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.rakam.presto.MemoryTracker;

import javax.inject.Inject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryBuffer
{
    private final MemoryTracker memoryTracker;
    Map<String, DynamicSliceOutput> files;

    @Inject
    public InMemoryBuffer(MemoryTracker memoryTracker)
    {
        this.memoryTracker = memoryTracker;
        files = new ConcurrentHashMap<>();
    }

    public TrackedDynamicSliceOutput create(String fileName, int bufferSize)
    {
        TrackedDynamicSliceOutput output = new TrackedDynamicSliceOutput(memoryTracker, bufferSize);
        DynamicSliceOutput previous = files.put(fileName, output);
        if (previous != null) {
            throw new IllegalStateException("Files are not immutable.");
        }
        return output;
    }

    public Slice get(String fileName)
    {
        DynamicSliceOutput output = files.get(fileName);
        if (output == null) {
            throw new IllegalStateException();
        }
        return output.slice();
    }

    public DynamicSliceOutput remove(String name)
    {
        return files.remove(name);
    }
}
