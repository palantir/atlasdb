package com.palantir.atlasdb.cli.services;

import java.util.Set;

import javax.net.ssl.SSLSocketFactory;

import org.immutables.value.Value;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.table.description.Schema;

@Value.Immutable
public abstract class ServicesConfig {

    public abstract AtlasDbConfig atlasDbConfig();

    @Value.Derived
    public AtlasDbFactory atlasDbFactory() {
        return TransactionManagers.getKeyValueServiceFactory(atlasDbConfig().keyValueService().type());
    }

    @Value.Default
    public Set<Schema> schemas() {
        return ImmutableSet.of();
    }

    @Value.Default
    public boolean allowAccessToHiddenTables() {
        return true;
    }

    public abstract Optional<SSLSocketFactory> sslSocketFactory();

}
