package com.palantir.atlasdb.memory.spi;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampService;

public class InMemoryAtlasDbFactory implements AtlasDbFactory<InMemoryKeyValueService> {

    @Override
    public String getType() {
        return "memory";
    }

    @Override
    public InMemoryKeyValueService createRawKeyValueService(JsonNode config) throws IOException {
        return new InMemoryKeyValueService(false);
    }

    @Override
    public TimestampService createTimestampService(InMemoryKeyValueService rawKvs) {
        return new InMemoryTimestampService();
    }

}
