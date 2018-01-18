/*
 * Licensed under the Rakam Incorporation
 */

package com.facebook.presto.raptor.storage;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.rakam.presto.MemoryTracker;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryFileSystem
        extends FileSystem
{
    private final MemoryTracker memoryTracker;
    Map<String, DynamicSliceOutput> files;

    @Inject
    public InMemoryFileSystem(MemoryTracker memoryTracker)
    {
        this.memoryTracker = memoryTracker;
        files = new ConcurrentHashMap<>();
    }

    @Override
    public URI getUri()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FSDataInputStream open(Path path, int i)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FSDataOutputStream create(Path path, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException
    {
        TrackedDynamicSliceOutput output = new TrackedDynamicSliceOutput(memoryTracker, bufferSize);
        DynamicSliceOutput previous = files.put(path.getName(), output);
        if (previous != null) {
            throw new IllegalStateException("Files are not immutable.");
        }
        return new FSDataOutputStream(output);
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, int i, short i1, long l, Progressable progressable)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean rename(Path path, Path path1)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean delete(Path path, boolean b)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileStatus[] listStatus(Path path)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Path getWorkingDirectory()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setWorkingDirectory(Path path)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileStatus getFileStatus(Path path)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public Slice remove(String fileName)
    {
        DynamicSliceOutput output = files.get(fileName);

        memoryTracker.freeMemory(output.getRetainedSize());
        files.remove(fileName);
        return output.slice();
    }

    public Slice get(String fileName)
    {
        DynamicSliceOutput output = files.get(fileName);
        if (output == null) {
            throw new IllegalStateException();
        }
        return output.slice();
    }
}
