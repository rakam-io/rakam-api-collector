/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import io.airlift.configuration.Config;

public class CommitterConfig
{
    private int committerThreadCount = 2;

    public int getCommitterThreadCount()
    {
        return committerThreadCount;
    }

    @Config("committer.thread.count")
    public void setCommitterThreadCount(int committerThreadCount)
    {
        this.committerThreadCount = committerThreadCount;
    }
}
