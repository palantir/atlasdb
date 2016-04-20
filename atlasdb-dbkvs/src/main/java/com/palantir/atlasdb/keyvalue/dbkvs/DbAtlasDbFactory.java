package com.palantir.atlasdb.keyvalue.dbkvs;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.timestamp.TimestampService;

public class DbAtlasDbFactory implements AtlasDbFactory {
    @Override
    public String getType() {
        return "db";
    }

    @Override
    public KeyValueService createRawKeyValueService(KeyValueServiceConfig config) {
        Preconditions.checkArgument(config instanceof DbKeyValueServiceConfiguration,
                "DbAtlasDbFactory expects a configuration of type DbKeyValueServiceConfiguration, found %s", config.getClass());
        return null;
    }

    @Override
    public TimestampService createTimestampService(KeyValueService rawKvs) {
        return null;
    }
}
