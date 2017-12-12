/*
 * Licensed under the Rakam Incorporation
 */

package com.facebook.presto.raptor.storage.backup;

import com.facebook.presto.rakam.S3BackupConfig;
import com.facebook.presto.raptor.backup.BackupStore;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.configuration.ConfigBinder;

public class S3BackupStoreModule implements Module {
    public S3BackupStoreModule() {
    }

    public void configure(Binder binder) {
        ConfigBinder.configBinder(binder).bindConfig(S3BackupConfig.class);
        binder.bind(BackupStore.class).to(S3BackupStore.class).in(Scopes.SINGLETON);
    }
}
