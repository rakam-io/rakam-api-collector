/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import io.airlift.configuration.Config;

import java.io.File;

public class CommitterConfig
{
    private int committerThreadCount = Runtime.getRuntime().availableProcessors();
    private File duplicateHandlerRocksdbDirectory;

    public int getCommitterThreadCount()
    {
        return committerThreadCount;
    }

    @Config("committer.thread.count")
    public void setCommitterThreadCount(int committerThreadCount)
    {
        this.committerThreadCount = committerThreadCount;
    }

    @Config("committer.duplicate-handler-rocksdb-directory")
    public void setDuplicateHandlerRocksdbDirectory(File duplicateHandlerRocksdbDirectory)
    {
        this.duplicateHandlerRocksdbDirectory = duplicateHandlerRocksdbDirectory;
    }

    public File getDuplicateHandlerRocksdbDirectory()
    {
        return duplicateHandlerRocksdbDirectory;
    }
}
