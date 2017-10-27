/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.connector.raptor;

import io.airlift.configuration.Config;

import java.io.File;
import java.net.URI;

public class RaptorConfig
{
    private String metadataUrl;
    private String nodeIdentifier;
    private File dataDirectory;
    private URI prestoURL;
    private String backupThreads = "5";

    public String getMetadataUrl()
    {
        return metadataUrl;
    }

    public String getBackupThreads() {return backupThreads;}

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

    @Config("raptor.backup.threads")
    public RaptorConfig setBackupThreads(String backupThreads)
    {
        if (backupThreads != null) {
            this.backupThreads = backupThreads;
        }
        return this;
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
