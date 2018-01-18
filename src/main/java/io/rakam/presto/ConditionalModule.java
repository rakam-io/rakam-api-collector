/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.configuration.ConfigurationAwareModule;
import io.airlift.configuration.ConfigurationFactory;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkState;

public class ConditionalModule
        implements ConfigurationAwareModule
{
    public static ConfigurationAwareModule installIfPropertyEquals(Module module, String property, String expectedValue)
    {
        return new ConditionalModule(module, property, expectedValue);
    }

    private final Module module;
    private final String property;
    private final String expectedValue;
    private ConfigurationFactory configurationFactory;

    private ConditionalModule(Module module, String property, String expectedValue)
    {
        this.module = Objects.requireNonNull(module, "module is null");
        this.property = Objects.requireNonNull(property, "property is null");
        this.expectedValue = Objects.requireNonNull(expectedValue, "expectedValue is null");
    }

    @Override
    public void setConfigurationFactory(ConfigurationFactory configurationFactory)
    {
        this.configurationFactory = Objects.requireNonNull(configurationFactory, "configurationFactory is null");
        configurationFactory.consumeProperty(property);
        if (module instanceof AbstractConfigurationAwareModule) {
            ((AbstractConfigurationAwareModule) module).setConfigurationFactory(configurationFactory);
        }

        // consume properties if we are not going to install the module
        if (!shouldInstall()) {
            configurationFactory.registerConfigurationClasses(module);
        }
    }

    @Override
    public void configure(Binder binder)
    {
        checkState(configurationFactory != null, "configurationFactory was not set");
        if (!configurationFactory.getProperties().containsKey(property)) {
            binder.addError("Required configuration property '%s' was not set", property);
        }
        if (shouldInstall()) {
            binder.install(module);
        }
    }

    private boolean shouldInstall()
    {
        return expectedValue.equalsIgnoreCase(configurationFactory.getProperties().get(property));
    }
}
