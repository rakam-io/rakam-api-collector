/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

public class
MiddlewareConfig
{
    private Duration maxFlushDuration = Duration.valueOf("60s");
    private int maxFlushRecords = 150_000;
    private DataSize maxSize = DataSize.succinctDataSize(350, DataSize.Unit.MEGABYTE);

    @Config("middleware.max-flush-duration")
    public MiddlewareConfig setMaxFlushDuration(Duration maxFlushDuration)
    {
        this.maxFlushDuration = maxFlushDuration;
        return this;
    }

    @Config("middleware.max-flush-records")
    public MiddlewareConfig setMaxFlushRecords(int maxFlushRecords)
    {
        this.maxFlushRecords = maxFlushRecords;
        return this;
    }

    @Config("middleware.max-size")
    public MiddlewareConfig setMaxSize(DataSize maxSize)
    {
        this.maxSize = maxSize;
        return this;
    }

    public DataSize getMaxSize()
    {
        return maxSize;
    }

    public Duration getMaxFlushDuration()
    {
        return maxFlushDuration;
    }

    public int getMaxFlushRecords()
    {
        return maxFlushRecords;
    }
}
