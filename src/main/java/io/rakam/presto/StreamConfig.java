/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

public class StreamConfig
{
    private Duration maxFlushDuration = Duration.succinctDuration(5, TimeUnit.SECONDS);
    private double maxFlushTotalMemoryRatio = .1;

    public Duration getMaxFlushDuration()
    {
        return maxFlushDuration;
    }

    @Config("stream.max-flush-duration")
    public StreamConfig setMaxFlushDuration(Duration maxFlushDuration)
    {
        this.maxFlushDuration = maxFlushDuration;
        return this;
    }

    @Config("stream.memory-max-flush-ratio")
    public void setMaxFlushTotalMemoryRatio(double maxFlushTotalMemoryRatio)
    {
        this.maxFlushTotalMemoryRatio = maxFlushTotalMemoryRatio;
    }

    public double getMaxFlushTotalMemoryRatio()
    {
        return maxFlushTotalMemoryRatio;
    }
}
