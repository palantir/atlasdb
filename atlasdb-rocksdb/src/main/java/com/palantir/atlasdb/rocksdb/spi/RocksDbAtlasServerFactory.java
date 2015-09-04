package com.palantir.atlasdb.rocksdb.spi;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.palantir.atlasdb.keyvalue.rocksdb.impl.RocksDbBoundStore;
import com.palantir.atlasdb.keyvalue.rocksdb.impl.RocksDbKeyValueService;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.timestamp.PersistentTimestampService;
import com.palantir.timestamp.TimestampService;

public class RocksDbAtlasServerFactory implements AtlasDbFactory<RocksDbKeyValueService> {

    @Override
    public String getType() {
        return "rocksdb";
    }

    @Override
    public RocksDbKeyValueService createRawKeyValueService(JsonNode config) throws IOException {
        String dataDir = config == null ? "rocksdb" : config.get("dataDir").asText();
        return RocksDbKeyValueService.create(dataDir);
    }

    @Override
    public TimestampService createTimestampService(RocksDbKeyValueService rawKvs) {
        return PersistentTimestampService.create(RocksDbBoundStore.create(rawKvs));
    }
}
