/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.connector.raptor;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.configuration.ConfigBinder;
import io.rakam.presto.DatabaseHandler;

public class RaptorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        ConfigBinder.configBinder(binder).bindConfig(RaptorConfig.class);
        binder.bind(DatabaseHandler.class).to(RaptorDatabaseHandler.class);
    }
}
