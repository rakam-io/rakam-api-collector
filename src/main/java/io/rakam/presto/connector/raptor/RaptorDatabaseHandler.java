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
import com.facebook.presto.raptor.RaptorConnector;
import com.facebook.presto.raptor.RaptorConnectorFactory;
import com.facebook.presto.raptor.RaptorModule;
import com.facebook.presto.raptor.backup.BackupModule;
import com.facebook.presto.raptor.metadata.DatabaseMetadataModule;
import com.facebook.presto.raptor.storage.InMemoryBuffer;
import com.facebook.presto.raptor.storage.IngestOnlyStorageModule;
import com.facebook.presto.raptor.storage.backup.RemoteBackupManager;
import com.facebook.presto.raptor.storage.backup.S3BackupStoreModule;
import com.facebook.presto.raptor.util.RebindSafeMBeanServer;
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
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.sql.gen.OrderingCompiler;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Files;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.json.JsonModule;
import io.airlift.slice.Slice;
import io.rakam.presto.DatabaseHandler;
import io.rakam.presto.FieldNameConfig;
import io.rakam.presto.MemoryTracker;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.config.JDBCConfig;
import org.rakam.config.ProjectConfig;
import org.rakam.presto.analysis.PrestoConfig;
import org.rakam.presto.analysis.PrestoRakamRaptorMetastore;
import org.weakref.jmx.guice.MBeanModule;

import javax.inject.Inject;
import javax.management.MBeanServer;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.transaction.IsolationLevel.READ_COMMITTED;
import static com.facebook.presto.spi.type.ParameterKind.TYPE;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.Locale.ENGLISH;
import static org.rakam.presto.analysis.PrestoQueryExecution.fromPrestoType;

public class RaptorDatabaseHandler
        implements DatabaseHandler
{
    private static final String RAKAM_RAPTOR_CONNECTOR = "RAKAM_RAPTOR_CONNECTOR";
    private final ConnectorMetadata metadata;
    private final ConnectorSession session;
    private final ConnectorTransactionHandle connectorTransactionHandle;
    private final ConnectorPageSinkProvider pageSinkProvider;
    private final PrestoRakamRaptorMetastore metastore;
    private final Supplier<ConnectorMetadata> writeMetadata;

    @Inject
    public RaptorDatabaseHandler(RaptorConfig config, TypeManager typeManager, S3BackupConfig s3BackupConfig, FieldNameConfig fieldNameConfig, MemoryTracker memoryTracker)
    {
        this(config, typeManager, s3BackupConfig, fieldNameConfig, memoryTracker, null);
    }

    public RaptorDatabaseHandler(RaptorConfig config, TypeManager typeManager, S3BackupConfig s3BackupConfig, FieldNameConfig fieldNameConfig, MemoryTracker memoryTracker, Module backupStoreModule)
    {
        DatabaseMetadataModule metadataModule = new DatabaseMetadataModule();

        ImmutableMap.Builder<String, Module> builder = ImmutableMap.builder();
        builder.put("s3", new S3BackupStoreModule());
        if (backupStoreModule != null) {
            builder.put("embedded", backupStoreModule);
        }
        ImmutableMap<String, Module> backupProviders = builder.build();

        RaptorConnectorFactory raptorConnectorFactory = new RaptorConnectorFactory(
                RAKAM_RAPTOR_CONNECTOR,
                metadataModule,
                backupProviders)
        {
            @Override
            public Connector create(String connectorId, Map<String, String> config, ConnectorContext context)
            {
                NodeManager nodeManager = context.getNodeManager();
                try {
                    Bootstrap app = new Bootstrap(
                            new JsonModule(),
                            new MBeanModule(),
                            binder -> {
                                MBeanServer mbeanServer = new RebindSafeMBeanServer(getPlatformMBeanServer());
                                binder.bind(MBeanServer.class).toInstance(mbeanServer);
                                binder.bind(NodeManager.class).toInstance(nodeManager);
                                binder.bind(PageSorter.class).toInstance(context.getPageSorter());
                                binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                            },
                            metadataModule,
                            new BackupModule(backupProviders),
                            new IngestOnlyStorageModule(connectorId),
                            new RaptorModule(connectorId),
                            new AbstractConfigurationAwareModule()
                            {
                                @Override
                                protected void setup(Binder binder)
                                {
                                    binder.bind(MemoryTracker.class).toInstance(memoryTracker);
                                }
                            },
                            binder -> binder.bind(InMemoryBuffer.class).asEagerSingleton(),
                            binder -> binder.bind(RemoteBackupManager.class).in(Scopes.SINGLETON)
                    );

                    Injector injector = app
                            .strictConfig()
                            .doNotInitializeLogging()
                            .setRequiredConfigurationProperties(config)
                            .initialize();

                    return injector.getInstance(RaptorConnector.class);
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        ImmutableMap.Builder<String, String> props = ImmutableMap.<String, String>builder()
                .put("metadata.db.type", "mysql")
                .put("metadata.db.url", config.getMetadataUrl())
                // No effect
                .put("backup.threads", String.valueOf(Runtime.getRuntime().availableProcessors() * 3))
                .put("storage.data-directory", Files.createTempDir().getAbsolutePath())
                .put("metadata.db.connections.max", String.valueOf(config.getMaxConnection()))
                .put("backup.timeout", "20m");

        if (backupStoreModule != null) {
            props.put("backup.provider", "embedded");
        }
        else if (s3BackupConfig.getS3Bucket() != null) {
            props.put("backup.provider", "s3");
            props.put("aws.s3-bucket", s3BackupConfig.getS3Bucket());
            props.put("aws.region", s3BackupConfig.getAWSRegion().getName());

            if (s3BackupConfig.getAccessKey() != null) {
                props.put("aws.access-key", s3BackupConfig.getAccessKey());
            }

            if (s3BackupConfig.getSecretAccessKey() != null) {
                props.put("aws.secret-access-key", s3BackupConfig.getSecretAccessKey());
            }

            if (s3BackupConfig.getEndpoint() != null) {
                props.put("aws.s3-endpoint", s3BackupConfig.getEndpoint());
            }
        }

        ImmutableMap<String, String> properties = props.build();

        NodeManager nodeManager = new SingleNodeManager(config.getNodeIdentifier());

        PagesIndexPageSorter pageSorter = new PagesIndexPageSorter(
                new PagesIndex.DefaultFactory(new OrderingCompiler(), new JoinCompiler(), new FeaturesConfig()));

        Connector connector = raptorConnectorFactory.create(RAKAM_RAPTOR_CONNECTOR, properties,
                new ProxyConnectorContext(nodeManager, typeManager, pageSorter));

        connectorTransactionHandle = connector.beginTransaction(READ_COMMITTED, false);

        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager();
        ConnectorId connectorId = new ConnectorId(RAKAM_RAPTOR_CONNECTOR);
        sessionPropertyManager.addConnectorSessionProperties(connectorId, connector.getSessionProperties());

        session = Session.builder(sessionPropertyManager)
                .setIdentity(new Identity("rakam", Optional.empty()))
                .setTimeZoneKey(UTC_KEY)
                .setLocale(ENGLISH)
                .setQueryId(QueryId.valueOf("streaming_batch"))
                .setCatalog(RAKAM_RAPTOR_CONNECTOR).build().toConnectorSession(connectorId);

        metadata = connector.getMetadata(connectorTransactionHandle);
        writeMetadata = () -> {
            ConnectorTransactionHandle connectorTransactionHandle = connector.beginTransaction(READ_COMMITTED, false);
            return connector.getMetadata(connectorTransactionHandle);
        };

        pageSinkProvider = connector.getPageSinkProvider();
        JDBCConfig jdbcConfig = new JDBCConfig();
        jdbcConfig.setConnectionMaxLifeTime(60000L);
        try {
            String uri = new URI(config.getMetadataUrl().substring(5)).getQuery();
            for (String elem : uri.split("&")) {
                String[] split = elem.split("=", 2);
                if (split[0].equals("user")) {
                    jdbcConfig.setUsername(URLDecoder.decode(split[1], "UTF-8"));
                }
                else if (split[0].equals("password")) {
                    jdbcConfig.setPassword(URLDecoder.decode(split[1], "UTF-8"));
                }
            }
            jdbcConfig.setUrl(config.getMetadataUrl());
            jdbcConfig.setMaxConnection(config.getMaxConnection() / 3);
        }
        catch (URISyntaxException | UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

        if (config.getPrestoURL() != null) {
            JDBCPoolDataSource orCreateDataSource = JDBCPoolDataSource.getOrCreateDataSource(jdbcConfig);
            PrestoConfig prestoConfig = new PrestoConfig();
            prestoConfig.setAddress(config.getPrestoURL());
            prestoConfig.setCheckpointColumn(fieldNameConfig.getCheckpointField());

            metastore = new PrestoRakamRaptorMetastore(orCreateDataSource,
                    new EventBus(), new ProjectConfig()
                    .setTimeColumn(fieldNameConfig.getTimeField())
                    .setUserColumn(fieldNameConfig.getUserFieldName()), prestoConfig);
        }
        else {
            metastore = null;
        }
    }

    @Override
    public List<ColumnMetadata> getColumns(String schema, String table)
    {
        Map<SchemaTableName, List<ColumnMetadata>> map = listColumns(new SchemaTablePrefix(schema, table), 5);
        if (map.isEmpty()) {
            throw new IllegalArgumentException("Table doesn't exist");
        }

        return map.entrySet().iterator().next().getValue();
    }

    private Map<SchemaTableName, List<ColumnMetadata>> listColumns(SchemaTablePrefix prefix, int tryCount)
    {
        try {
            return metadata.listTableColumns(session, prefix);
        }
        catch (Exception e) {
            if (tryCount > 0) {
                return listColumns(prefix, tryCount - 1);
            }
            throw e;
        }
    }

    @Override
    public List<ColumnMetadata> addColumns(String schema, String table, List<ColumnMetadata> columns)
    {
        if (metastore == null) {
            throw new IllegalStateException();
        }
        metastore.getOrCreateCollectionFields(schema, table, columns.stream().map(e -> {
            TypeSignature typeSignature = e.getType().getTypeSignature();
            FieldType type = fromPrestoType(typeSignature.getBase(),
                    typeSignature.getParameters().stream()
                            .filter(input -> input.getKind() == TYPE)
                            .map(typeSignatureParameter -> typeSignatureParameter.getTypeSignature().getBase()).iterator());
            return new SchemaField(e.getName(), type);
        }).collect(Collectors.toSet()));

        return getColumns(schema, table);
    }

    @Override
    public Inserter insert(String schema, String table)
    {
        // 2 Mysql queries
        ConnectorTableHandle tableHandle = metadata.getTableHandle(session, new SchemaTableName(schema, table));
        ConnectorMetadata connectorMetadata = writeMetadata.get();
        // 2 Mysql queries
        ConnectorInsertTableHandle insertTableHandle = connectorMetadata.beginInsert(session, tableHandle);

        ConnectorPageSink pageSink = pageSinkProvider.createPageSink(connectorTransactionHandle, session, insertTableHandle);

        return new Inserter()
        {
            @Override
            public void addPage(Page page)
            {
                pageSink.appendPage(page);
            }

            @Override
            public CompletableFuture<Void> commit()
            {
                CompletableFuture<Collection<Slice>> finish = pageSink.finish();
                return finish.thenAccept(slices ->
                        // 6 mysql insert queries
                        connectorMetadata.finishInsert(session, insertTableHandle, slices));
            }
        };
    }

    private static class ProxyConnectorContext
            implements ConnectorContext
    {
        private final NodeManager nodeManager;
        private final TypeManager typeManager;
        private final PagesIndexPageSorter pageSorter;

        public ProxyConnectorContext(NodeManager nodeManager, TypeManager typeManager, PagesIndexPageSorter pageSorter)
        {
            this.nodeManager = nodeManager;
            this.typeManager = typeManager;
            this.pageSorter = pageSorter;
        }

        public NodeManager getNodeManager()
        {
            return nodeManager;
        }

        public TypeManager getTypeManager()
        {
            return typeManager;
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
