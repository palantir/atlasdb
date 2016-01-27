package com.palantir.atlasdb.spi;

import com.fasterxml.jackson.databind.ObjectMapper;

public interface AtlasDbServerEnvironment {
    void register(Object resource);
    ObjectMapper getObjectMapper();
}