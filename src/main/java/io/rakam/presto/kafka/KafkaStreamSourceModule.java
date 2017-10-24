/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.rakam.presto.StreamConfig;
import io.rakam.presto.deserialization.MessageEventTransformer;
import io.rakam.presto.kinesis.KinesisStreamSourceModule;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.rakam.presto.ConditionalModule.installIfPropertyEquals;
import static java.lang.String.format;

public class KafkaStreamSourceModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        KafkaConfig config = buildConfigObject(KafkaConfig.class);

        binder.bind(KafkaWorkerManager.class).in(Scopes.SINGLETON);

        Class<? extends MessageEventTransformer> clazz;
        switch (config.getDataFormat()) {
            case AVRO:
                clazz = KafkaJsonMessageTransformer.class;
                break;
            case JSON:
                clazz = KafkaAvroMessageTransformer.class;
                break;
            default:
                throw new IllegalStateException(format("The data format %s is not supported.", config.getDataFormat().toString()));
        }

        binder.bind(MessageEventTransformer.class).to(clazz).in(Scopes.SINGLETON);
    }
}
