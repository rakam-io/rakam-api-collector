/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.connector.redshift;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class RedshiftConfig
{
    private String url;

    @NotNull
    public String getUrl()
    {
        return url;
    }

    @Config("redshift.url")
    public RedshiftConfig setUrl(String url)
    {
        if (url != null && url.length() > 0) {
            this.url = url;
        }
        return this;
    }
}
