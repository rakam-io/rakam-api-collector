/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.connector.s3;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.configuration.ConfigBinder;
import io.rakam.presto.DatabaseHandler;
import io.rakam.presto.connector.raptor.RaptorConfig;

public class S3Module
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        ConfigBinder.configBinder(binder).bindConfig(S3TargetConfig.class);
        ConfigBinder.configBinder(binder).bindConfig(RaptorConfig.class);
        binder.bind(DatabaseHandler.class).to(S3DatabaseHandler.class).asEagerSingleton();
    }
}
