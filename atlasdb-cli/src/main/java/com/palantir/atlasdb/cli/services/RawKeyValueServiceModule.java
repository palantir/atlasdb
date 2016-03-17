package com.palantir.atlasdb.cli.services;

import javax.inject.Named;
import javax.inject.Singleton;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;

import dagger.Module;
import dagger.Provides;

@Module
public class RawKeyValueServiceModule {

    @Provides
    @Singleton
    @Named("rawKvs")
    public KeyValueService provideRawKeyValueService(ServicesConfig config) {
        return config.atlasDbFactory().createRawKeyValueService(config.atlasDbConfig().keyValueService());
    }

}
