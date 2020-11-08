package io.rakam.presto.connector.custom;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.rakam.presto.DatabaseHandler;

public class CustomModule
        extends AbstractConfigurationAwareModule {
    @Override
    protected void setup(Binder binder) {
        binder.bind(DatabaseHandler.class).to(CustomDatabaseHandler.class).asEagerSingleton();
    }
}