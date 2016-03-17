package com.palantir.atlasdb.cli.services;

import javax.inject.Named;
import javax.inject.Singleton;

import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.spi.AtlasDbFactory;

import dagger.Module;
import dagger.Provides;

@Module
public class RawKeyValueServiceModule {

    @Provides
    @Singleton
    @Named("rawKvs")
    public KeyValueService provideRawKeyValueService(AtlasDbFactory kvsFactory, AtlasDbConfig config) {
        return kvsFactory.createRawKeyValueService(config.keyValueService());
    }

}
