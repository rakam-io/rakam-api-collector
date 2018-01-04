/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDataSize;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class StreamConfig {
    private Duration maxFlushDuration = Duration.succinctDuration(5, TimeUnit.SECONDS);
    private int realTimeFlushDays = 1;

    @Config("stream.max-flush-duration")
    public StreamConfig setMaxFlushDuration(Duration maxFlushDuration) {
        this.maxFlushDuration = maxFlushDuration;
        return this;
    }

    public Duration getMaxFlushDuration() {
        return maxFlushDuration;
    }

    @Config("stream.real-time-flush-duration")
    public StreamConfig setRealTimeFlushDays(int realTimeFlushDays) {
        if (realTimeFlushDays < 1) {
            throw new IllegalStateException("`stream.real-time-flush-duration` must be greater than 1");
        }
        this.realTimeFlushDays = realTimeFlushDays;
        return this;
    }

    public int getRealTimeFlushDays() {
        return realTimeFlushDays;
    }
}
