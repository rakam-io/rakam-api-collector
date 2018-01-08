package com.facebook.presto.raptor.storage;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

public class HistoricalDataConfig
{
    private Duration maxFlushDuration = Duration.succinctDuration(30, TimeUnit.MINUTES);
    private int maxFlushRecords = 500;

    @Config("historical.max-flush-duration")
    public HistoricalDataConfig setMaxFlushDuration(Duration maxFlushDuration)
    {
        this.maxFlushDuration = maxFlushDuration;
        return this;
    }

    public Duration getMaxFlushDuration()
    {
        return maxFlushDuration;
    }

    @Config("historical.max-flush-records")
    public HistoricalDataConfig setMaxFlushRecords(int maxFlushRecords)
    {
        this.maxFlushRecords = maxFlushRecords;
        return this;
    }

    public int getMaxFlushRecords()
    {
        return maxFlushRecords;
    }
}
