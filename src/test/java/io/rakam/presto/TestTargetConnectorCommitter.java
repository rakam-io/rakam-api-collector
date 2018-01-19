/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.Session;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.connector.informationSchema.InformationSchemaConnector;
import com.facebook.presto.connector.system.SystemConnector;
import com.facebook.presto.metadata.*;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.connector.*;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.testing.TestingSession;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.rakam.presto.MiddlewareBuffer.TableCheckpoint;
import io.rakam.presto.deserialization.TableData;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static com.facebook.presto.connector.ConnectorId.createInformationSchemaConnectorId;
import static com.facebook.presto.connector.ConnectorId.createSystemTablesConnectorId;
import static io.rakam.presto.BlockAssertions.createLongsBlock;
import static io.rakam.presto.BlockAssertions.createStringsBlock;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.fail;

public class TestTargetConnectorCommitter
{
    private CatalogManager catalogManager;
    private TransactionManager testTransactionManager;
    private MetadataManager testMetadataManager;
    private Session session;

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

    @BeforeMethod
    @AfterMethod
    private void reset()
    {
        catalogManager = new CatalogManager();
        testTransactionManager = TransactionManager.createTestTransactionManager(catalogManager);
        testMetadataManager = createTestMetadataManager(testTransactionManager);
        session = TestingSession.testSessionBuilder().setCatalog("testconnector").build();
    }

    @Test
    public void testCommitter()
            throws Exception
    {
        CountDownLatch latch = new CountDownLatch(3);

        TestingMetadata connectorMetadata = new TestingMetadata();

        catalogManager.registerCatalog(createTestingCatalog("testconnector", new ConnectorId("testconnector"),
                new TestingConnector(connectorMetadata),
                testTransactionManager, testMetadataManager));

        connectorMetadata.createTable(session.toConnectorSession(),
                new ConnectorTableMetadata(new SchemaTableName("test", "test"), ImmutableList.of()), false);

        TestDatabaseHandler databaseHandler = new TestDatabaseHandler("test", "test", ImmutableList.of());
        databaseHandler.setLatchForInsert(latch);
        TargetConnectorCommitter committer = new TargetConnectorCommitter(databaseHandler);

        SchemaTableName table = new SchemaTableName("test", "test");
        TableData tableData = new TableData(new Page(1), ImmutableList.of());
        BatchRecords batchRecords = new BatchRecords(ImmutableMap.of(table, tableData), () -> latch.countDown());

        ImmutableList<TableCheckpoint> checkpoints = ImmutableList.of(new TableCheckpoint(batchRecords, table));
        committer.process(table, checkpoints).whenComplete(generate(checkpoints));
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testMultipleCommitter()
            throws Exception
    {
        CountDownLatch latch = new CountDownLatch(4);

        TestingMetadata connectorMetadata = new TestingMetadata();
        connectorMetadata.createTable(session.toConnectorSession(),
                new ConnectorTableMetadata(new SchemaTableName("test", "test0"), ImmutableList.of()), false);
        connectorMetadata.createTable(session.toConnectorSession(),
                new ConnectorTableMetadata(new SchemaTableName("test", "test1"), ImmutableList.of()), false);

        catalogManager.registerCatalog(createTestingCatalog("testconnector", new ConnectorId("testconnector"),
                new TestingConnector(connectorMetadata),
                testTransactionManager, testMetadataManager));

        TestDatabaseHandler databaseHandler = new TestDatabaseHandler("test", "test", ImmutableList.of());
        databaseHandler.setLatchForInsert(latch);
        TargetConnectorCommitter committer = new TargetConnectorCommitter(databaseHandler);

        SchemaTableName table0 = new SchemaTableName("test", "test0");
        SchemaTableName table1 = new SchemaTableName("test", "test1");

        TableData tableData = new TableData(new Page(1), ImmutableList.of());

        ImmutableList<TableCheckpoint> checkpoints0 = ImmutableList.of(new TableCheckpoint(new BatchRecords(ImmutableMap.of(table0, tableData),
                () -> latch.countDown()), table0));
        ImmutableList<TableCheckpoint> checkpoints1 = ImmutableList.of(new TableCheckpoint(new BatchRecords(ImmutableMap.of(table1, tableData),
                () -> latch.countDown()), table1));
        committer.process(table0, checkpoints0).whenComplete(generate(checkpoints0));
        committer.process(table1, checkpoints1).whenComplete(generate(checkpoints1));

        assertTrue(latch.await(4, TimeUnit.SECONDS));
    }

    @Test
    public void testSchemaChange()
            throws Exception
    {
        CountDownLatch latch = new CountDownLatch(4);
        TestingMetadata connectorMetadata = new TestingMetadata();

        catalogManager.registerCatalog(createTestingCatalog("testconnector", new ConnectorId("testconnector"),
                new TestingConnector(connectorMetadata),
                testTransactionManager, testMetadataManager));

        ImmutableList<ColumnMetadata> schema = ImmutableList.of(
                new ColumnMetadata("test1", VarcharType.VARCHAR),
                new ColumnMetadata("test2", BigintType.BIGINT));
        connectorMetadata.createTable(session.toConnectorSession(),
                new ConnectorTableMetadata(new SchemaTableName("test", "test"), schema), false);

        TestDatabaseHandler databaseHandler = new TestDatabaseHandler("test", "test", ImmutableList.of());
        databaseHandler.setLatchForInsert(latch);
        TargetConnectorCommitter committer = new TargetConnectorCommitter(databaseHandler);

        TableData page1 = new TableData(new Page(createStringsBlock("test")), ImmutableList.of(new ColumnMetadata("test1", VarcharType.VARCHAR)));
        TableData page2 = new TableData(new Page(createStringsBlock("test"), createLongsBlock(1)), schema);
        SchemaTableName table = new SchemaTableName("test", "test");
        BatchRecords batchRecords1 = new BatchRecords(ImmutableMap.of(table, page1), () -> latch.countDown());
        BatchRecords batchRecords2 = new BatchRecords(ImmutableMap.of(table, page2), () -> latch.countDown());

        ImmutableList<TableCheckpoint> checkpoints = ImmutableList.of(
                new TableCheckpoint(batchRecords1, table),
                new TableCheckpoint(batchRecords2, table));
        committer.process(table, checkpoints).whenComplete(generate(checkpoints));

        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    public BiConsumer<Void, Throwable> generate(ImmutableList<TableCheckpoint> checkpoints)
    {
        return (aVoid, throwable) -> {
            if (throwable != null) {
                throw new IllegalStateException(throwable);
            }

            for (TableCheckpoint tableCheckpoint : checkpoints) {
                try {
                    tableCheckpoint.checkpoint();
                }
                catch (BatchRecords.CheckpointException e) {
                    throw new IllegalStateException(e);
                }
            }
        };
    }

    private static class TestingConnectorPageSinkProvider
            implements ConnectorPageSinkProvider {
        private final CountDownLatch latch;

        public TestingConnectorPageSinkProvider(CountDownLatch latch)
        {
            this.latch = latch;
        }

        @Override
        public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle) {
            fail();
            return null;
        }

        @Override
        public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle) {
            latch.countDown();

            return new ConnectorPageSink()
            {
                @SuppressWarnings("Duplicates")
                @Override
                public CompletableFuture appendPage(Page page) {
                    latch.countDown();
                    return CompletableFuture.completedFuture(null);
                }

                @Override
                public CompletableFuture<Collection<Slice>> finish() {
                    latch.countDown();
                    return CompletableFuture.completedFuture(ImmutableList.of());
                }

                @Override
                public void abort() {
                    fail();
                }
            };
        }
    }

    private static class NoOpConnectorPageSinkProvider
            implements ConnectorPageSinkProvider
    {

        public NoOpConnectorPageSinkProvider()
        {
        }

        @Override
        public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
        {
            fail();
            return null;
        }

        @Override
        public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
        {
            return new ConnectorPageSink()
            {
                @SuppressWarnings("Duplicates")
                @Override
                public CompletableFuture appendPage(Page page)
                {
                    return CompletableFuture.completedFuture(null);
                }

                @Override
                public CompletableFuture<Collection<Slice>> finish()
                {
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
            extends TestingMetadata {
        private final CountDownLatch latch;

        public LatchTestingMetadata(CountDownLatch latch)
        {
            this.latch = latch;
        }

        @Override
        public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle) {
            return new TestingConnectorInsertTableHandle(tableHandle);
        }

        @Override
        public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments) {
            latch.countDown();
            return Optional.empty();
        }
    }

    private static class TestingConnectorInsertTableHandle
            implements ConnectorInsertTableHandle {

        private final ConnectorTableHandle tableHandle;

        public TestingConnectorInsertTableHandle(ConnectorTableHandle inMemoryTableHandle) {
            this.tableHandle = inMemoryTableHandle;
        }
    }

    private static class TestingConnector
            implements Connector {
        private final TestingMetadata connectorMetadata;

        public TestingConnector(TestingMetadata connectorMetadata)
        {
            this.connectorMetadata = connectorMetadata;
        }

        @Override
        public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly) {
            return TestingTransactionHandle.create();
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
            return connectorMetadata;
        }

        @Override
        public ConnectorSplitManager getSplitManager() {
            return null;
        }
    }

}
