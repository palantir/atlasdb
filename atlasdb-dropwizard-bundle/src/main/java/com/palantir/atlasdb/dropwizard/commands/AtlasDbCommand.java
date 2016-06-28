package com.palantir.atlasdb.dropwizard.commands;

import com.palantir.atlasdb.dropwizard.AtlasDbConfigurationProvider;

import io.dropwizard.Configuration;
import io.dropwizard.cli.ConfiguredCommand;

public abstract class AtlasDbCommand<T extends Configuration & AtlasDbConfigurationProvider> extends ConfiguredCommand<T> {
    private final Class<T> configurationClass;

    protected AtlasDbCommand(String name, String description, Class<T> configurationClass) {
        super(name, description);

        this.configurationClass = configurationClass;
    }

    @Override
    protected Class<T> getConfigurationClass() {
        return configurationClass;
    }
}
