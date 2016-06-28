package com.palantir.atlasdb.dropwizard;

import com.palantir.atlasdb.config.AtlasDbConfig;

public interface AtlasDbConfigurationProvider {
    AtlasDbConfig getAtlasConfig();
}
