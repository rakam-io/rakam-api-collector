/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.connector.s3;

import com.google.inject.Binder;
import com.google.inject.name.Names;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.configuration.ConfigBinder;
import io.rakam.presto.DatabaseHandler;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.config.JDBCConfig;

public class S3Module
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        ConfigBinder.configBinder(binder).bindConfig(S3TargetConfig.class);

        JDBCPoolDataSource metadataDataSource = JDBCPoolDataSource.getOrCreateDataSource(buildConfigObject(JDBCConfig.class, "metadata.store.jdbc"));
        binder.bind(JDBCPoolDataSource.class).annotatedWith(Names.named("metadata.store.jdbc")).toInstance(metadataDataSource);

        binder.bind(DatabaseHandler.class).to(S3DatabaseHandler.class).asEagerSingleton();
    }
}
