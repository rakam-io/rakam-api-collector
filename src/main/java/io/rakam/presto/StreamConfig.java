/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.hadoop.$internal.com.google.common.base.Strings;
import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

public class StreamConfig
{
    private Duration maxFlushDuration = Duration.succinctDuration(5, TimeUnit.SECONDS);
    private int memoryMultiplier = 2;

    @Config("stream.max-flush-duration")
    public StreamConfig setMaxFlushDuration(Duration maxFlushDuration)
    {
        this.maxFlushDuration = maxFlushDuration;
        return this;
    }

    public Duration getMaxFlushDuration()
    {
        return maxFlushDuration;
    }

    public int getMemoryMultiplier()
    {
        return memoryMultiplier;
    }

    @Config("stream.memory-multiplier")
    public void setMemoryMultiplier(String memoryMultiplier)
    {
        if (!Strings.isNullOrEmpty(memoryMultiplier)) {
            this.memoryMultiplier = Integer.parseInt(memoryMultiplier);
        }
    }
}
