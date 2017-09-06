/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.Session;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.connector.informationSchema.InformationSchemaConnector;
import com.facebook.presto.connector.system.SystemConnector;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.SchemaPropertyManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.split.PageSinkManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.testing.TestingSession;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;
import io.airlift.slice.Slice;
import io.rakam.presto.deserialization.TableData;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.connector.ConnectorId.createInformationSchemaConnectorId;
import static com.facebook.presto.connector.ConnectorId.createSystemTablesConnectorId;
import static com.google.common.collect.ImmutableTable.of;
import static io.rakam.presto.BlockAssertions.createLongsBlock;
import static io.rakam.presto.BlockAssertions.createStringsBlock;
import static org.testng.AssertJUnit.fail;

public class TestTargetConnectorCommitter
{
    @Test
    public void testCommitter()
            throws Exception
    {
        CountDownLatch latch = new CountDownLatch(3);
        CatalogManager catalogManager = new CatalogManager();
        TransactionManager testTransactionManager = TransactionManager.createTestTransactionManager(catalogManager);
        Session session = TestingSession.testSessionBuilder().setCatalog("testconnector").build();

        MetadataManager testMetadataManager = createTestMetadataManager(testTransactionManager);
        TestingMetadata connectorMetadata = new LatchTestingMetadata(latch);
        connectorMetadata.createTable(session.toConnectorSession(),
                new ConnectorTableMetadata(new SchemaTableName("test", "test"), ImmutableList.of()));

        catalogManager.registerCatalog(createTestingCatalog("testconnector", new ConnectorId("testconnector"),
                new TestingConnector(connectorMetadata),
                testTransactionManager, testMetadataManager));

        PageSinkManager pageSinkManager = new PageSinkManager();
        pageSinkManager.addConnectorPageSinkProvider(new ConnectorId("testconnector"), new TestingConnectorPageSinkProvider(latch));

        TargetConnectorCommitter committer = new TargetConnectorCommitter(new TestDatabaseHandler("test", "test", ImmutableList.of()));

        committer.process(ImmutableList.of(of("test", "test", new TableData(new Page(1), ImmutableList.of()))));

        latch.await(1, TimeUnit.SECONDS);
    }

    //@Test
    public void testSchemaChange()
            throws Exception
    {
        CountDownLatch latch = new CountDownLatch(3);
        TestingMetadata connectorMetadata = new LatchTestingMetadata(latch);

        CatalogManager catalogManager = new CatalogManager();
        TransactionManager testTransactionManager = TransactionManager.createTestTransactionManager(catalogManager);
        MetadataManager testMetadataManager = createTestMetadataManager(testTransactionManager);

        catalogManager.registerCatalog(createTestingCatalog("testconnector", new ConnectorId("testconnector"),
                new TestingConnector(connectorMetadata),
                testTransactionManager, testMetadataManager));

        Session session = TestingSession.testSessionBuilder().setCatalog("testconnector").build();

        ImmutableList<ColumnMetadata> schema = ImmutableList.of(
                new ColumnMetadata("test1", VarcharType.VARCHAR),
                new ColumnMetadata("test2", BigintType.BIGINT));
        connectorMetadata.createTable(session.toConnectorSession(),
                new ConnectorTableMetadata(new SchemaTableName("test", "test"), schema));

        PageSinkManager pageSinkManager = new PageSinkManager();
        pageSinkManager.addConnectorPageSinkProvider(new ConnectorId("testconnector"), new TestingConnectorPageSinkProvider(latch));

        TargetConnectorCommitter committer = new TargetConnectorCommitter(new TestDatabaseHandler("test", "test", ImmutableList.of()));

        List<Table<String, String, TableData>> batches = ImmutableList.of(
                of("test", "test", new TableData(new Page(createStringsBlock("test")), ImmutableList.of(new ColumnMetadata("test1", VarcharType.VARCHAR)))),
                of("test", "test", new TableData(new Page(createStringsBlock("test"), createLongsBlock(1)), schema)));

        committer.process(batches);

        latch.await(1, TimeUnit.SECONDS);
    }

    private static class TestingConnectorPageSinkProvider
            implements ConnectorPageSinkProvider
    {
        private final CountDownLatch latch;

        public TestingConnectorPageSinkProvider(CountDownLatch latch) {this.latch = latch;}

        @Override
        public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
        {
            fail();
            return null;
        }

        @Override
        public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
        {
            latch.countDown();
            return new ConnectorPageSink()
            {
                @Override
                public CompletableFuture appendPage(Page page)
                {
                    latch.countDown();
                    return CompletableFuture.completedFuture(null);
                }

                @Override
                public CompletableFuture<Collection<Slice>> finish()
                {
                    latch.countDown();
                    return CompletableFuture.completedFuture(ImmutableList.of());
                }

                @Override
                public void abort()
                {
                    fail();
                }
            };
        }
    }

    private static class LatchTestingMetadata
            extends TestingMetadata
    {
        private final CountDownLatch latch;

        public LatchTestingMetadata(CountDownLatch latch) {this.latch = latch;}

        @Override
        public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            return new TestingConnectorInsertTableHandle(tableHandle);
        }

        @Override
        public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments)
        {
            latch.countDown();
            return Optional.empty();
        }
    }

    private static class TestingConnectorInsertTableHandle
            implements ConnectorInsertTableHandle
    {

        private final ConnectorTableHandle tableHandle;

        public TestingConnectorInsertTableHandle(ConnectorTableHandle inMemoryTableHandle)
        {
            this.tableHandle = inMemoryTableHandle;
        }
    }

    private static class TestingConnector
            implements Connector
    {
        private final TestingMetadata connectorMetadata;

        public TestingConnector(TestingMetadata connectorMetadata) {this.connectorMetadata = connectorMetadata;}

        @Override
        public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
        {
            return TestingTransactionHandle.create();
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
        {
            return connectorMetadata;
        }

        @Override
        public ConnectorSplitManager getSplitManager()
        {
            return null;
        }
    }

    public static Catalog createTestingCatalog(String catalogName, ConnectorId connectorId, Connector connector, TransactionManager transactionManager, MetadataManager metadata)
    {
        ConnectorId systemId = createSystemTablesConnectorId(connectorId);
        InternalNodeManager nodeManager = new InMemoryNodeManager();
        AllowAllAccessControl allowAllAccessControl = new AllowAllAccessControl();
        return new Catalog(
                catalogName,
                connectorId,
                connector,
                createInformationSchemaConnectorId(connectorId),
                new InformationSchemaConnector(catalogName, nodeManager, metadata, allowAllAccessControl),
                systemId,
                new SystemConnector(
                        systemId,
                        nodeManager,
                        connector.getSystemTables(),
                        transactionId -> transactionManager.getConnectorTransaction(transactionId, connectorId)));
    }

    public static MetadataManager createTestMetadataManager(TransactionManager transactionManager)
    {
        FeaturesConfig featuresConfig = new FeaturesConfig();
        TypeManager typeManager = new TypeRegistry();
        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager();
        BlockEncodingSerde blockEncodingSerde = new BlockEncodingManager(typeManager);
        return new MetadataManager(
                featuresConfig,
                typeManager,
                blockEncodingSerde,
                sessionPropertyManager,
                new SchemaPropertyManager(),
                new TablePropertyManager(),
                transactionManager);
    }
}
