/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.raptor.backup.BackupStore;
import com.facebook.presto.raptor.storage.InMemoryBuffer;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import io.airlift.slice.Slice;

import java.io.File;
import java.util.UUID;
import java.util.function.BiConsumer;

public class TestBackupStoreModule
        implements Module
{
    private final BiConsumer<UUID, Slice> function;

    public TestBackupStoreModule(BiConsumer<UUID, Slice> function)
    {
        this.function = function;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(new TypeLiteral<BiConsumer<UUID, Slice>>() {}).toInstance(function);
        binder.bind(BackupStore.class).to(TestBackupStore.class).in(Scopes.SINGLETON);
    }

    public static class TestBackupStore
            implements BackupStore
    {

        private final InMemoryBuffer buffer;
        private final BiConsumer<UUID, Slice> function;

        @Inject
        public TestBackupStore(BiConsumer<UUID, Slice> function, InMemoryBuffer buffer)
        {
            this.function = function;
            this.buffer = buffer;
        }

        @Override
        public void backupShard(UUID uuid, File source)
        {
            function.accept(uuid, buffer.remove(source.getName()));
        }

        @Override
        public void restoreShard(UUID uuid, File target)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean deleteShard(UUID uuid)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean shardExists(UUID uuid)
        {
            throw new UnsupportedOperationException();
        }
    }
}
