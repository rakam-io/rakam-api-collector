/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class StreamConfig
{
    private Duration maxFlushDuration = Duration.succinctDuration(5, TimeUnit.SECONDS);
    private int maxFlushRecords = 10_000;
    private DataSize maxFlushDataSize = DataSize.succinctDataSize(100, MEGABYTE);
    private Duration realTimeIngestionDuration = Duration.valueOf("1d");

    @Config("stream.max-flush-duration")
    public StreamConfig setMaxFlushDuration(Duration maxFlushDuration)
    {
        this.maxFlushDuration = maxFlushDuration;
        return this;
    }

    public Duration getRealtimeIngestionDuration() {
        return realTimeIngestionDuration;
    }

    @Config("stream.real-time-ingestion-duration")
    public StreamConfig setRealtimeIngestionDuration(Duration realTimeIngestionDuration)
    {
        this.realTimeIngestionDuration = realTimeIngestionDuration;
        return this;
    }

    @Config("stream.max-flush-records")
    public StreamConfig setMaxFlushRecords(int maxFlushRecords)
    {
        this.maxFlushRecords = maxFlushRecords;
        return this;
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
