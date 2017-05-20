/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.connector.raptor;

import io.airlift.configuration.Config;

import java.io.File;

public class RaptorConfig
{
    private String metadataUrl;
    private String nodeIdentifier;
    private File dataDirectory;

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

    public File getDataDirectory()
    {
        return dataDirectory;
    }

    @Config("raptor.storage.data-directory")
    public RaptorConfig setDataDirectory(File dataDirectory)
    {
        this.dataDirectory = dataDirectory;
        return this;
    }
}
