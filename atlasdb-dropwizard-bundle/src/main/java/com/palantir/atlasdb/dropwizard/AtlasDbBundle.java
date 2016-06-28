package com.palantir.atlasdb.dropwizard;

import io.dropwizard.Configuration;
import io.dropwizard.ConfiguredBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class AtlasDbBundle<T extends Configuration & AtlasDbConfigurationProvider> implements ConfiguredBundle<T> {
    @Override
    public void initialize(Bootstrap<?> bootstrap) {
        Class<T> configurationClass = (Class<T>) bootstrap.getApplication().getConfigurationClass();
        bootstrap.addCommand(new AtlasDbConfiguredCommand<T>(configurationClass));
    }

    @Override
    public void run(T configuration, Environment environment) throws Exception {
    }
}
