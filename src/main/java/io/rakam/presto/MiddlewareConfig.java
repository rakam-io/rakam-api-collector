/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

public class MiddlewareConfig
{
    private Duration maxFlushDuration = Duration.valueOf("60s");

    @Config("middleware.max-flush-duration")
    public MiddlewareConfig setMaxFlushDuration(Duration maxFlushDuration)
    {
        this.maxFlushDuration = maxFlushDuration;
        return this;
    }

    public Duration getMaxFlushDuration()
    {
        return maxFlushDuration;
    }
}
