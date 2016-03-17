package com.palantir.atlasdb.cli.services;

import java.util.Set;

import javax.inject.Singleton;
import javax.net.ssl.SSLSocketFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.table.description.Schema;

import dagger.Module;
import dagger.Provides;

@Module
public class ServicesConfigModule {

    private final AtlasDbConfig config;

    public ServicesConfigModule(AtlasDbConfig config) {
        this.config = config;
    }

    @Provides
    @Singleton
    public AtlasDbConfig provideAtlasDbConfig() { return config; }

    @Provides
    @Singleton
    public Set<Schema> provideSchemas() {
        return ImmutableSet.of();
    }

    @Provides
    public boolean provideAllowAccessToHiddenTables() {
        return true;
    }

    @Provides
    @Singleton
    public AtlasDbFactory provideAtlasDbFactory(AtlasDbConfig config) {
        return TransactionManagers.getKeyValueServiceFactory(config.keyValueService().type());
    }

    @Provides
    public Optional<SSLSocketFactory> provideSslSocketFactory() {
        return Optional.absent();
    }

}
