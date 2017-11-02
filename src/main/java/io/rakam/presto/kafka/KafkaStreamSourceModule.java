/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.configuration.Config;
import io.rakam.presto.deserialization.MessageEventTransformer;
import io.rakam.presto.deserialization.json.JsonDeserializer;

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
                clazz = KafkaAvroMessageTransformer.class;
                break;
            case JSON:
                JsonConfig jsonConfig = buildConfigObject(JsonConfig.class);
                binder.bind(JsonDeserializer.class).to(jsonConfig.getDataLayout().getJsonDeserializerClass()).in(Scopes.SINGLETON);
                clazz = KafkaJsonMessageTransformer.class;
                break;
            default:
                throw new IllegalStateException(format("The data format %s is not supported.", config.getDataFormat().toString()));
        }

        binder.bind(MessageEventTransformer.class).to(clazz).in(Scopes.SINGLETON);
    }

}
