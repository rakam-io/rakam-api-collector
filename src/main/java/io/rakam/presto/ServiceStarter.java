/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.log.Logger;
import io.rakam.presto.connector.raptor.RaptorModule;
import io.rakam.presto.kafka.KafkaStreamSourceModule;
import io.rakam.presto.kinesis.KinesisStreamSourceModule;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.rakam.presto.ConditionalModule.installIfPropertyEquals;

public final class ServiceStarter {
    public static String RAKAM_VERSION;
    private final static Logger LOGGER = Logger.get(ServiceStarter.class);

    static {
        Properties properties = new Properties();
        InputStream inputStream;
        try {
            URL resource = ServiceStarter.class.getResource("/git.properties");
            if (resource == null) {
                LOGGER.warn("git.properties doesn't exist.");
            } else {
                inputStream = resource.openStream();
                properties.load(inputStream);
            }
        } catch (IOException e) {
            LOGGER.warn(e, "Error while reading git.properties");
        }
        try {
            RAKAM_VERSION = properties.get("git.commit.id.describe-short").toString().split("-", 2)[0];
        } catch (Exception e) {
            LOGGER.warn(e, "Error while parsing git.properties");
        }
    }

    private ServiceStarter()
            throws InstantiationException {
        throw new InstantiationException("The class is not created for instantiation");
    }

    public static void main(String[] args)
            throws Throwable {
        if (args.length > 0) {
            System.setProperty("config", args[0]);
        }

        Bootstrap app = new Bootstrap(
                new StreamSourceModule(),
                new LogModule(),
                new RaptorModule(), new Module() {
            @Override
            public void configure(Binder binder) {
                TypeRegistry typeRegistry = new TypeRegistry();
                binder.bind(TypeManager.class).toInstance(typeRegistry);

                BlockEncodingManager blockEncodingManager = new BlockEncodingManager(typeRegistry, ImmutableSet.of());
                binder.bind(BlockEncodingSerde.class).toInstance(blockEncodingManager);

                FunctionRegistry functionRegistry = new FunctionRegistry(typeRegistry, blockEncodingManager, new FeaturesConfig());
                binder.bind(FunctionRegistry.class).toInstance(functionRegistry);
            }
        });

        app.requireExplicitBindings(false);
        app.strictConfig().initialize();

        LOGGER.info("======== SERVER STARTED ========");
    }

    public static class StreamSourceModule
            extends AbstractConfigurationAwareModule {
        @Override
        protected void setup(Binder binder) {
            configBinder(binder).bindConfig(StreamConfig.class);
            configBinder(binder).bindConfig(FieldNameConfig.class);
            configBinder(binder).bindConfig(S3MiddlewareConfig.class);
            configBinder(binder).bindConfig(MiddlewareConfig.class);
            binder.bind(StreamWorkerContext.class).in(Scopes.SINGLETON);
            binder.bind(TargetConnectorCommitter.class).in(Scopes.SINGLETON);

            bindDataSource("stream.source");
        }

        private void bindDataSource(String sourceName) {
            install(installIfPropertyEquals(new KafkaStreamSourceModule(), sourceName, "kafka"));
            install(installIfPropertyEquals(new KinesisStreamSourceModule(), sourceName, "kinesis"));
        }
    }
}