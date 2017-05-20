/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.rakam.presto.MessageEventTransformer;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class KafkaStreamSourceModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(KafkaConfig.class);
        binder.bind(KafkaWorkerManager.class).in(Scopes.SINGLETON);
        binder.bind(MessageEventTransformer.class).to(KafkaMessageTransformer.class).in(Scopes.SINGLETON);
    }
}
