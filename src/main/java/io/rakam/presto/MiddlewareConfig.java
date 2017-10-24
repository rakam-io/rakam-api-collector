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
    public void setMaxFlushDuration(Duration maxFlushDuration)
    {
        this.maxFlushDuration = maxFlushDuration;
    }

    @Config("middleware.max-flush-records")
    public void setMaxFlushRecords(int maxFlushRecords)
    {
        this.maxFlushRecords = maxFlushRecords;
    }

    @Config("middleware.max-size")
    public void setMaxSize(DataSize maxSize)
    {
        this.maxSize = maxSize;
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
