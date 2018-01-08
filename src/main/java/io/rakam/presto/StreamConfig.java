/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDataSize;

import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class StreamConfig
{
    private Duration maxFlushDuration = Duration.succinctDuration(5, TimeUnit.SECONDS);

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
}
