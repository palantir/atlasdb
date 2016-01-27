package com.palantir.atlasdb.rocksdb;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.remoting.RemotingKeyValueService;
import com.palantir.atlasdb.spi.AtlasDbServerEnvironment;
import com.palantir.atlasdb.spi.AtlasDbServicePlugin;

@JsonDeserialize(as = ImmutableRocksDbServicePlugin.class)
@JsonSerialize(as = ImmutableRocksDbServicePlugin.class)
@JsonTypeName(RocksDbKeyValueServiceConfig.TYPE)
@Value.Immutable
public abstract class RocksDbServicePlugin implements AtlasDbServicePlugin {
    @Override
    public String type() {
        return RocksDbKeyValueServiceConfig.TYPE;
    }

    @Override
    public void registerServices(AtlasDbServerEnvironment environment) {
        KeyValueService kvs = new RocksDbAtlasDbFactory().createRawKeyValueService(getConfig());
        kvs.initializeFromFreshInstance();
        RemotingKeyValueService.registerKeyValueWithEnvironment(kvs, environment);
    }

    public abstract RocksDbKeyValueServiceConfig getConfig();

}
