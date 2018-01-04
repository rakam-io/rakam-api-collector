/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.connector.raptor;

import io.airlift.configuration.Config;

import java.io.File;
import java.net.URI;
import java.net.URL;

public class RaptorConfig
{
    private String metadataUrl;
    private String nodeIdentifier = "collector";
    private URI prestoURL;
    private int maxConnection = 100;

    public String getMetadataUrl()
    {
        return metadataUrl;
    }

    @Config("raptor.metadata.url")
    public RaptorConfig setMetadataUrl(String metadataUrl)
    {
        this.metadataUrl = metadataUrl;
        return this;
    }

    public int getMaxConnection() {
        return maxConnection;
    }

    @Config("raptor.metadata.max-connection")
    public RaptorConfig setMaxConnection(int maxConnection)
    {
        this.maxConnection = maxConnection;
        return this;
    }

    public String getNodeIdentifier()
    {
        return nodeIdentifier;
    }

    @Config("raptor.node.id")
    public RaptorConfig setNodeIdentifier(String nodeIdentifier)
    {
        this.nodeIdentifier = nodeIdentifier;
        return this;
    }

    public URI getPrestoURL()
    {
        return prestoURL;
    }

    @Config("raptor.presto-url")
    public RaptorConfig setPrestoURL(URI prestoURL)
    {
        this.prestoURL = prestoURL;
        return this;
    }
}
