package io.rakam.presto;

import com.facebook.presto.raptor.backup.BackupStore;
import com.google.inject.Binder;
import com.google.inject.Module;

import java.io.File;
import java.util.UUID;
import java.util.function.BiConsumer;

public class TestBackupStoreModule implements Module {
    private final BiConsumer<UUID, File> function;

    public TestBackupStoreModule(BiConsumer<UUID, File> function) {
        this.function = function;
    }

    @Override
    public void configure(Binder binder) {
        binder.bind(BackupStore.class).toInstance(new TestBackupStore());
    }

    public class TestBackupStore implements BackupStore {
        @Override
        public void backupShard(UUID uuid, File source) {
            function.accept(uuid, source);
        }

        @Override
        public void restoreShard(UUID uuid, File target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean deleteShard(UUID uuid) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean shardExists(UUID uuid) {
            throw new UnsupportedOperationException();
        }
    }
}
