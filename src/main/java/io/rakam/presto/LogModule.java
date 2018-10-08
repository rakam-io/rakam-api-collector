/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.getsentry.raven.jul.SentryHandler;
import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.configuration.Config;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class LogModule
        extends AbstractConfigurationAwareModule
{
    private static final String SENTRY_DSN = "https://96145b6c6517416e9bcd24bba96e4433:6f5ba5b727114d5e91694b1755584500@sentry.io/1290995";

    @Override
    public void setup(Binder binder)
    {
        LogManager manager = LogManager.getLogManager();
        LogConfig logConfig = buildConfigObject(LogConfig.class);
        if (logConfig.getLogActive() && !Arrays.stream(manager.getLogger("").getHandlers()).anyMatch(e -> e instanceof SentryHandler)) {
            Logger rootLogger = manager.getLogger("");

            SentryHandler sentryHandler = new SentryHandler();
            sentryHandler.setDsn(SENTRY_DSN);
            sentryHandler.setTags(logConfig.getTags());
            sentryHandler.setLevel(Level.SEVERE);

            URL gitProps = LogUtil.class.getResource("/git.properties");

            if (gitProps != null) {
                Properties properties = new Properties();
                try {
                    properties.load(gitProps.openStream());
                }
                catch (IOException e) {
                }

                sentryHandler.setRelease(properties.get("git.commit.id.describe").toString());
            }
            rootLogger.addHandler(sentryHandler);
        }
    }

    public static class LogConfig
    {
        private boolean logActive = true;
        private String tags;

        public boolean getLogActive()
        {
            return logActive;
        }

        @Config("log-active")
        public LogConfig setLogActive(boolean logActive)
        {
            this.logActive = logActive;
            return this;
        }

        public String getTags()
        {
            return tags;
        }

        @Config("log-identifier")
        public LogConfig setTags(String tags)
        {
            this.tags = tags;
            return this;
        }
    }
}
