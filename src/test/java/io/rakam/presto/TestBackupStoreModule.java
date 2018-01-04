/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.raptor.backup.BackupStore;
import com.facebook.presto.raptor.storage.InMemoryFileSystem;
import com.google.inject.*;

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
        binder.bind(new TypeLiteral<BiConsumer<UUID, File>>(){}).toInstance(function);
        binder.bind(BackupStore.class).to(TestBackupStore.class).in(Scopes.SINGLETON);
    }

    public static class TestBackupStore implements BackupStore {

        private final InMemoryFileSystem inMemoryFileSystem;
        private final BiConsumer<UUID, File> function;

        @Inject
        public TestBackupStore(BiConsumer<UUID, File> function, InMemoryFileSystem inMemoryFileSystem) {
            this.function = function;
            this.inMemoryFileSystem = inMemoryFileSystem;
        }

        @Override
        public void backupShard(UUID uuid, File source) {
            function.accept(uuid, source);
            inMemoryFileSystem.remove(source.getName());
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
