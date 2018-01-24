/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.raptor.backup.BackupConfig;
import com.facebook.presto.raptor.util.RebindSafeMBeanServer;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.OptionalBinder;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.log.LoggingConfiguration;
import io.rakam.presto.connector.raptor.RaptorModule;
import io.rakam.presto.kafka.KafkaStreamSourceModule;
import io.rakam.presto.kinesis.KinesisStreamSourceModule;
import org.weakref.jmx.guice.MBeanModule;

import javax.management.MBeanServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.Properties;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.rakam.presto.ConditionalModule.installIfPropertyEquals;
import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public final class ServiceStarter
{
    private final static Logger LOGGER = Logger.get(ServiceStarter.class);
    public static String RAKAM_VERSION;

    private ServiceStarter()
            throws InstantiationException
    {
        throw new InstantiationException("The class is not created for instantiation");
    }

    public static void main(String[] args)
            throws Throwable
    {
        if (args.length > 0) {
            System.setProperty("config", args[0]);
        }

        initializeLogging(System.getProperty("log.levels-file"));

        Bootstrap app = new Bootstrap(
                new StreamSourceModule(),
                new LogModule(),
                new MBeanModule(),
                new RaptorModule(), new Module()
        {
            @Override
            public void configure(Binder binder)
            {
                MBeanServer mbeanServer = new RebindSafeMBeanServer(getPlatformMBeanServer());
                binder.bind(MBeanServer.class).toInstance(mbeanServer);

                newExporter(binder).export(MemoryTracker.class).as(generatedNameOf(MemoryTracker.class));

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

    public static void initializeLogging(String logLevelsFile)
    {
        // unhook out and err while initializing logging or logger will print to them
        PrintStream out = System.out;
        PrintStream err = System.err;

        try {
            LoggingConfiguration config = new LoggingConfiguration();

            if (logLevelsFile != null) {
                config.setLevelsFile(logLevelsFile);
            }

            Logging logging = Logging.initialize();
            logging.configure(config);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        finally {
            System.setOut(out);
            System.setErr(err);
        }
    }

    public static class StreamSourceModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            configBinder(binder).bindConfig(StreamConfig.class);
            configBinder(binder).bindConfig(BackupConfig.class);
            configBinder(binder).bindConfig(FieldNameConfig.class);
            configBinder(binder).bindConfig(S3MiddlewareConfig.class);
            configBinder(binder).bindConfig(CommitterConfig.class);
            configBinder(binder).bindConfig(MiddlewareConfig.class);
            binder.bind(StreamWorkerContext.class).in(Scopes.SINGLETON);
            binder.bind(TargetConnectorCommitter.class).in(Scopes.SINGLETON);

            OptionalBinder.newOptionalBinder(binder, HistoricalDataHandler.class);

            bindDataSource("stream.source");
        }

        private void bindDataSource(String sourceName)
        {
            install(installIfPropertyEquals(new KafkaStreamSourceModule(), sourceName, "kafka"));
            install(installIfPropertyEquals(new KinesisStreamSourceModule(), sourceName, "kinesis"));
        }
    }

    static {
        Properties properties = new Properties();
        InputStream inputStream;
        try {
            URL resource = ServiceStarter.class.getResource("/git.properties");
            if (resource == null) {
                LOGGER.warn("git.properties doesn't exist.");
            }
            else {
                inputStream = resource.openStream();
                properties.load(inputStream);
            }
        }
        catch (IOException e) {
            LOGGER.warn(e, "Error while reading git.properties");
        }
        try {
            RAKAM_VERSION = properties.get("git.commit.id.describe-short").toString().split("-", 2)[0];
        }
        catch (Exception e) {
            LOGGER.warn(e, "Error while parsing git.properties");
        }
    }
}