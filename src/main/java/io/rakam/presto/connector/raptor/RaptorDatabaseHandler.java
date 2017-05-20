/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.connector.raptor;

import com.facebook.presto.PagesIndexPageSorter;
import com.facebook.presto.Session;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.PrestoNode;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.raptor.RaptorConnectorFactory;
import com.facebook.presto.raptor.metadata.DatabaseMetadataModule;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.sql.gen.OrderingCompiler;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Module;
import io.airlift.slice.Slice;
import io.rakam.presto.DatabaseHandler;

import javax.inject.Inject;

import java.net.URI;
import java.security.Principal;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.spi.transaction.IsolationLevel.READ_COMMITTED;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Locale.ENGLISH;

public class RaptorDatabaseHandler
        implements DatabaseHandler
{
    private static final String RAKAM_RAPTOR_CONNECTOR = "RAKAM_RAPTOR_CONNECTOR";
    private final ConnectorMetadata metadata;
    private final ConnectorSession session;
    private final ConnectorTransactionHandle connectorTransactionHandle;
    private final ConnectorPageSinkProvider pageSinkProvider;

    @Inject
    public RaptorDatabaseHandler(RaptorConfig config)
    {
        RaptorConnectorFactory raptorConnectorFactory = new RaptorConnectorFactory(
                RAKAM_RAPTOR_CONNECTOR,
                new DatabaseMetadataModule(),
                ImmutableMap.of("s3", (Module) binder -> {
                }));

        ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
                .put("metadata.db.type", "mysql")
                .put("metadata.db.url", config.getMetadataUrl())
                .put("storage.data-directory", config.getDataDirectory().getAbsolutePath())
                .put("metadata.db.connections.max", "200")
                .put("storage.compaction-enabled", "false")
                .put("storage.organization-enabled", "false")
                .put("backup.timeout", "20m").build();

        NodeManager nodeManager = new SingleNodeManager(config.getNodeIdentifier());

        PagesIndexPageSorter pageSorter = new PagesIndexPageSorter(
                new PagesIndex.DefaultFactory(new OrderingCompiler(), new JoinCompiler()));

        Connector connector = raptorConnectorFactory.create(RAKAM_RAPTOR_CONNECTOR, properties,
                new ProxyConnectorContext(nodeManager, new TypeRegistry(), pageSorter));

        connectorTransactionHandle = connector.beginTransaction(READ_COMMITTED, false);

        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager();
        ConnectorId connectorId = new ConnectorId(RAKAM_RAPTOR_CONNECTOR);
        sessionPropertyManager.addConnectorSessionProperties(connectorId, connector.getSessionProperties());

        session = Session.builder(sessionPropertyManager)
                .setIdentity(new Identity("rakam", Optional.<Principal>empty()))
                .setTimeZoneKey(UTC_KEY)
                .setLocale(ENGLISH)
                .setQueryId(QueryId.valueOf("streaming_batch"))
                .setCatalog(RAKAM_RAPTOR_CONNECTOR).build().toConnectorSession(connectorId);

        metadata = connector.getMetadata(connectorTransactionHandle);
        pageSinkProvider = connector.getPageSinkProvider();
    }

    @Override
    public List<ColumnMetadata> getColumns(String schema, String table)
    {
        Map<SchemaTableName, List<ColumnMetadata>> schemaTableNameListMap =
                metadata.listTableColumns(session, new SchemaTablePrefix(schema, table));
        List<ColumnMetadata> columnMetadatas = schemaTableNameListMap.get(new SchemaTableName(schema, table));
        if(columnMetadatas == null) {
            throw new IllegalArgumentException("Table doesn't exist");
        }

        return columnMetadatas;
    }

    @Override
    public Inserter insert(String schema, String table)
    {
        ConnectorTableHandle tableHandle = metadata.getTableHandle(session, new SchemaTableName(schema, table));
        ConnectorInsertTableHandle insertTableHandle = metadata.beginInsert(session, tableHandle);

        ConnectorPageSink pageSink = pageSinkProvider.createPageSink(connectorTransactionHandle, session, insertTableHandle);

        return new Inserter() {

            @Override
            public void addPage(Page page)
            {
                pageSink.appendPage(page);
            }

            @Override
            public CompletableFuture<Void> commit()
            {
                CompletableFuture<Collection<Slice>> finish = pageSink.finish();
                return finish.thenAccept(slices -> metadata.finishInsert(session, insertTableHandle, slices));
            }
        };
    }

    private static class ProxyConnectorContext
            implements ConnectorContext
    {
        private final NodeManager nodeManager;
        private final TypeRegistry typeRegistry;
        private final PagesIndexPageSorter pageSorter;

        public ProxyConnectorContext(NodeManager nodeManager, TypeRegistry typeRegistry, PagesIndexPageSorter pageSorter)
        {
            this.nodeManager = nodeManager;
            this.typeRegistry = typeRegistry;
            this.pageSorter = pageSorter;
        }

        public NodeManager getNodeManager()
        {
            return nodeManager;
        }

        public TypeManager getTypeManager()
        {
            return typeRegistry;
        }

        public PageSorter getPageSorter()
        {
            return pageSorter;
        }

        public PageIndexerFactory getPageIndexerFactory()
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class SingleNodeManager
            implements NodeManager
    {
        private final PrestoNode prestoNode;

        public SingleNodeManager(String nodeIdentifier)
        {
            this.prestoNode = new PrestoNode(nodeIdentifier, URI.create("http://127.0.0.1:8080"), NodeVersion.UNKNOWN, false);
        }

        @Override
        public Set<Node> getAllNodes()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<Node> getWorkerNodes()
        {
            return ImmutableSet.of();
        }

        @Override
        public Node getCurrentNode()
        {
            return prestoNode;
        }

        @Override
        public String getEnvironment()
        {
            throw new UnsupportedOperationException();
        }
    }
}
