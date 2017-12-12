/*
 * Licensed under the Rakam Incorporation
 */

package com.facebook.presto.raptor.storage;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import org.apache.hadoop.fs.*;
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
    Map<String, DynamicSliceOutput> files;

    @Inject
    public InMemoryFileSystem()
    {
        files = new ConcurrentHashMap<>();
    }

    @Override
    public URI getUri() {
        throw new UnsupportedOperationException();
    }

    @Override
    public FSDataInputStream open(Path path, int i) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FSDataOutputStream create(Path path, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException
    {
        DynamicSliceOutput output = new DynamicSliceOutput(bufferSize);
        files.put(path.getName(), output);
        return new FSDataOutputStream(output);
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, int i, short i1, long l, Progressable progressable) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean rename(Path path, Path path1) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean delete(Path path, boolean b) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setWorkingDirectory(Path path) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Path getWorkingDirectory() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        throw new UnsupportedOperationException();
    }

    public void remove(String fileName) {
        files.remove(fileName);
    }

    public Slice get(String fileName) {
        return files.get(fileName).slice();
    }
}