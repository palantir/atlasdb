/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
