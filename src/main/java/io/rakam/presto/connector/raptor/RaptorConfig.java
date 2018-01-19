/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.connector.raptor;

import io.airlift.configuration.Config;

import java.net.URI;

public class RaptorConfig
{
    private String metadataUrl;
    private String nodeIdentifier = "collector";
    private URI prestoURL;
    private String backupThreads = "5";
    private String dbMaxConnections = "100";
    private int maxConnection = 100;

    public String getMetadataUrl()
    {
        return metadataUrl;
    }

    @Config("raptor.metadata.url")
    public RaptorConfig setMetadataUrl(String metadataUrl)
    {
        if (metadataUrl != null && metadataUrl.length() > 0) {
            this.metadataUrl = metadataUrl;
        }
        return this;
    }

    public String getBackupThreads() {return backupThreads;}

    @Config("raptor.backup.threads")
    public RaptorConfig setBackupThreads(String backupThreads)
    {
        if (backupThreads != null) {
            this.backupThreads = backupThreads;
        }
        return this;
    }

    public String getDbMaxConnections() {return dbMaxConnections;}

    @Config("metadata.db.connections.max")
    public RaptorConfig setDbMaxConnections(String connections)
    {
        if (connections != null) {
            this.dbMaxConnections = connections;
        }
        return this;
    }

    public int getMaxConnection()
    {
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
        if (nodeIdentifier != null) {
            this.nodeIdentifier = nodeIdentifier;
        }
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
