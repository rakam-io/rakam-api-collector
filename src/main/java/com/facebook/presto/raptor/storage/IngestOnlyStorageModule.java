/*
 * Licensed under the Rakam Incorporation
 */

package com.facebook.presto.raptor.storage;

import com.facebook.presto.raptor.backup.BackupManager;
import com.facebook.presto.raptor.metadata.AssignmentLimiter;
import com.facebook.presto.raptor.metadata.DatabaseShardManager;
import com.facebook.presto.raptor.metadata.DatabaseShardRecorder;
import com.facebook.presto.raptor.metadata.MetadataConfig;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.ShardRecorder;
import com.google.common.base.Ticker;
import com.google.inject.Binder;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class IngestOnlyStorageModule
        extends StorageModule
{
    private final String connectorId;

    public IngestOnlyStorageModule(String connectorId)
    {
        super(connectorId);
        this.connectorId = connectorId;
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(StorageManagerConfig.class);
//        configBinder(binder).bindConfig(BucketBalancerConfig.class);
//        configBinder(binder).bindConfig(ShardCleanerConfig.class);
        configBinder(binder).bindConfig(MetadataConfig.class);

        binder.bind(Ticker.class).toInstance(Ticker.systemTicker());

        binder.bind(StorageManager.class).to(InMemoryOrcStorageManager.class).in(Scopes.SINGLETON);
//        binder.bind(StorageManager.class).to(OrcStorageManager.class).in(Scopes.SINGLETON);

        binder.bind(StorageService.class).to(FileStorageService.class).in(Scopes.SINGLETON);
        binder.bind(ShardManager.class).to(DatabaseShardManager.class).in(Scopes.SINGLETON);
        binder.bind(ShardRecorder.class).to(DatabaseShardRecorder.class).in(Scopes.SINGLETON);
        binder.bind(DatabaseShardManager.class).in(Scopes.SINGLETON);
        binder.bind(DatabaseShardRecorder.class).in(Scopes.SINGLETON);
//        binder.bind(ShardRecoveryManager.class).in(Scopes.SINGLETON);
        binder.bind(BackupManager.class).in(Scopes.SINGLETON);
//        binder.bind(ShardCompactionManager.class).in(Scopes.SINGLETON);
//        binder.bind(ShardOrganizationManager.class).in(Scopes.SINGLETON);
//        binder.bind(ShardOrganizer.class).in(Scopes.SINGLETON);
//        binder.bind(JobFactory.class).to(OrganizationJobFactory.class).in(Scopes.SINGLETON);
//        binder.bind(ShardCompactor.class).in(Scopes.SINGLETON);
//        binder.bind(ShardEjector.class).in(Scopes.SINGLETON);
//        binder.bind(ShardCleaner.class).in(Scopes.SINGLETON);
//        binder.bind(BucketBalancer.class).in(Scopes.SINGLETON);
        binder.bind(ReaderAttributes.class).in(Scopes.SINGLETON);
        binder.bind(AssignmentLimiter.class).in(Scopes.SINGLETON);

//        newExporter(binder).export(ShardRecoveryManager.class).as(generatedNameOf(ShardRecoveryManager.class, connectorId));
        newExporter(binder).export(BackupManager.class).as(generatedNameOf(BackupManager.class, connectorId));
        newExporter(binder).export(StorageManager.class).as(generatedNameOf(OrcStorageManager.class, connectorId));
//        newExporter(binder).export(ShardCompactionManager.class).as(generatedNameOf(ShardCompactionManager.class, connectorId));
//        newExporter(binder).export(ShardOrganizer.class).as(generatedNameOf(ShardOrganizer.class, connectorId));
//        newExporter(binder).export(ShardCompactor.class).as(generatedNameOf(ShardCompactor.class, connectorId));
//        newExporter(binder).export(ShardEjector.class).as(generatedNameOf(ShardEjector.class, connectorId));
//        newExporter(binder).export(ShardCleaner.class).as(generatedNameOf(ShardCleaner.class, connectorId));
//        newExporter(binder).export(BucketBalancer.class).as(generatedNameOf(BucketBalancer.class, connectorId));
//        newExporter(binder).export(JobFactory.class).withGeneratedName();
    }
}
