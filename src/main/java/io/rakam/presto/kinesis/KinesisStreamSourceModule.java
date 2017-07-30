/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.rakam.presto.deserialization.MessageEventTransformer;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class KinesisStreamSourceModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(KinesisStreamSourceConfig.class);
        binder.bind(MessageEventTransformer.class).to(KinesisMessageEventTransformer.class).in(Scopes.SINGLETON);
        binder.bind(IRecordProcessorFactory.class).to(KinesisRecordProcessorFactory.class);
        binder.bind(KinesisWorkerManager.class).asEagerSingleton();
    }
}
