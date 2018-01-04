/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto.kafka;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.rakam.presto.HistoricalDataHandler;
import io.rakam.presto.deserialization.DecoupleMessage;
import io.rakam.presto.deserialization.MessageEventTransformer;
import io.rakam.presto.deserialization.json.JsonDeserializer;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.lang.String.format;

public class KafkaStreamSourceModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        KafkaConfig config = buildConfigObject(KafkaConfig.class);
        configBinder(binder).bindConfig(JsonConfig.class);

        binder.bind(KafkaRealTimeWorker.class).in(Scopes.SINGLETON);

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

        binder.bind(DecoupleMessage.class).to(KafkaDecoupleMessage.class).in(Scopes.SINGLETON);
        binder.bind(HistoricalDataHandler.class).to(KafkaHistoricalDataHandler.class).in(Scopes.SINGLETON);
        binder.bind(MessageEventTransformer.class).to(clazz).in(Scopes.SINGLETON);
    }

}
