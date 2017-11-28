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
    private String dbMaxConnections = "100";

    public String getMetadataUrl()
    {
        return metadataUrl;
    }

    public String getBackupThreads() {return backupThreads;}

    public String getDbMaxConnections() {return dbMaxConnections;}

    @Config("raptor.metadata.url")
    public RaptorConfig setMetadataUrl(String metadataUrl)
    {
        if (metadataUrl != null && metadataUrl.length() > 0) {
            this.metadataUrl = metadataUrl;
        }
        return this;
    }

    @Config("metadata.db.connections.max")
    public RaptorConfig setDbMaxConnections(String connections)
    {
        if (connections != null) {
            this.dbMaxConnections = connections;
        }
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
        if (nodeIdentifier != null) {
            this.nodeIdentifier = nodeIdentifier;
        }
        return this;
    }

    public File getDataDirectory()
    {
        return dataDirectory;
    }

    @Config("raptor.storage.data-directory")
    public RaptorConfig setDataDirectory(File dataDirectory)
    {
        if (dataDirectory == null || dataDirectory.length() < 1) {
            throw new RuntimeException("storage directory cannot be null");
        }
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
        if (prestoURL != null) {
            this.prestoURL = prestoURL;
        }
        return this;
    }
}
