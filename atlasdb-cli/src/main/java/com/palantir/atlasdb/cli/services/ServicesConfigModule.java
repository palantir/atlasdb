package com.palantir.atlasdb.cli.services;

import javax.inject.Singleton;

import com.palantir.atlasdb.config.AtlasDbConfig;

import dagger.Module;
import dagger.Provides;

@Module
public class ServicesConfigModule {

    private final ServicesConfig config;

    public static ServicesConfigModule create(AtlasDbConfig atlasDbConfig) {
        return new ServicesConfigModule(ImmutableServicesConfig.builder().atlasDbConfig(atlasDbConfig).build());
    }

    public ServicesConfigModule(ServicesConfig config) {
        this.config = config;
    }

    @Provides
    @Singleton
    public ServicesConfig provideAtlasDbConfig() { return config; }

}
