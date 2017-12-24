/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class StreamConfig
{
    private Duration maxFlushDuration = Duration.succinctDuration(5, TimeUnit.SECONDS);
    private int maxFlushRecords = 10_000;
    private DataSize maxSizeOfView = DataSize.succinctDataSize(1, GIGABYTE);
    private DataSize maxFlushDataSize = DataSize.succinctDataSize(100, MEGABYTE);

    @Config("stream.max-flush-duration")
    public void setMaxFlushDuration(Duration maxFlushDuration)
    {
        this.maxFlushDuration = maxFlushDuration;
    }

    @Config("stream.max-flush-records")
    public void setMaxFlushRecords(int maxFlushRecords)
    {
        this.maxFlushRecords = maxFlushRecords;
    }

    public Duration getMaxFlushDuration()
    {
        return maxFlushDuration;
    }

    public DataSize getDataSize()
    {
        return maxFlushDataSize;
    }

    @Config("stream.max-flush-datasize")
    public StreamConfig setDataSize(DataSize maxFlushDataSize)
    {
        this.maxFlushDataSize = maxFlushDataSize;
        return this;
    }

    public int getMaxFlushRecords()
    {
        return maxFlushRecords;
    }
}
