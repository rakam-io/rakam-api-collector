/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.google.common.base.Splitter;
import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.configuration.Config;
import io.sentry.Sentry;
import io.sentry.SentryClient;
import io.sentry.jul.SentryHandler;
import org.rakam.util.RakamClient;

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
    private static final io.airlift.log.Logger log = io.airlift.log.Logger.get(LogModule.class);

    private static final String RELEASE;

    static {
        URL gitProps = org.rakam.util.LogUtil.class.getResource("/git.properties");

        if (gitProps != null) {
            Properties properties = new Properties();
            Object describe = null;
            try {
                properties.load(gitProps.openStream());
                describe = properties.get("git.commit.id.describe");
            }
            catch (IOException e) {
                log.warn(e, "Unable to fetch release tag");
            }
            finally {
                if (describe != null) {
                    RELEASE = describe.toString();
                }
                else {
                    RELEASE = null;
                }
            }
        }
        else {
            RELEASE = null;
            log.warn("Unable to fetch release tag because git.properties doesn't exist");
        }
    }

    private static final String SENTRY_DSN = String.format("https://96145b6c6517416e9bcd24bba96e4433@sentry.io/1290995?release=%s&stacktrace.app.packages=io.rakam",
            RakamClient.RELEASE);

    @Override
    public void setup(Binder binder)
    {
        LogConfig logConfig = buildConfigObject(LogConfig.class);

        if (logConfig.getLogActive()) {
            Sentry.init(SENTRY_DSN+"&tags=company:"+logConfig.getCompanyName());
        }
    }

    public static class LogConfig
    {
        private boolean logActive = true;
        private String tags;
        private String companyName;

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

        public String getCompanyName()
        {
            return companyName;
        }

        @Config("company-name")
        public LogConfig setCompanyName(String companyName)
        {
            this.companyName = companyName;
            return this;
        }
    }
}
