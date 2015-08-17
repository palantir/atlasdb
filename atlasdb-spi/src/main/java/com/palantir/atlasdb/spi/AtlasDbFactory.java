package com.palantir.atlasdb.spi;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.timestamp.TimestampService;

public interface AtlasDbFactory<KVS extends KeyValueService> {

    String getType();

    KVS createRawKeyValueService(JsonNode config) throws IOException;

    TimestampService createTimestampService(KVS rawKvs);
}
