/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.getsentry.raven.Raven;
import com.getsentry.raven.RavenFactory;
import com.getsentry.raven.jul.SentryHandler;
import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.logging.LogManager;
import java.util.stream.Collectors;

public class LogUtil
{
    private static final Raven RAVEN;
    private static final Map<String, String> TAGS;
    private static final String RELEASE;

    static {
        LogManager manager = LogManager.getLogManager();
        String canonicalName = SentryHandler.class.getCanonicalName();
        String dsnInternal = manager.getProperty(canonicalName + ".dsn");
        String tagsString = manager.getProperty(canonicalName + ".tags");

        TAGS = Optional.ofNullable(tagsString).map(str ->
                Arrays.stream(str.split(",")).map(val -> val.split(":")).collect(Collectors.toMap(a -> a[0], a -> a[1])))
                .orElse(ImmutableMap.of());

        RELEASE = manager.getProperty(canonicalName + ".release");

        RAVEN = dsnInternal != null ? RavenFactory.ravenInstance(dsnInternal) : null;
    }
}
