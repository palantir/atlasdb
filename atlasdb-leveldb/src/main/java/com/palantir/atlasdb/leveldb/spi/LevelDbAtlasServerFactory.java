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
package com.palantir.atlasdb.leveldb.spi;

import java.io.File;
import java.io.IOException;

import org.iq80.leveldb.DBException;

import com.fasterxml.jackson.databind.JsonNode;
import com.palantir.atlasdb.keyvalue.leveldb.impl.LevelDbBoundStore;
import com.palantir.atlasdb.keyvalue.leveldb.impl.LevelDbKeyValueService;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.common.base.Throwables;
import com.palantir.timestamp.PersistentTimestampService;
import com.palantir.timestamp.TimestampService;

public class LevelDbAtlasServerFactory implements AtlasDbFactory<LevelDbKeyValueService> {

    @Override
    public String getType() {
        return "leveldb";
    }

    @Override
    public LevelDbKeyValueService createRawKeyValueService(JsonNode config) throws IOException {
        String dataDir = config == null ? "leveldb" : config.get("dataDir").asText();
        return createKv(dataDir);
    }

    @Override
    public TimestampService createTimestampService(LevelDbKeyValueService rawKvs) {
        return PersistentTimestampService.create(LevelDbBoundStore.create(rawKvs));
    }

    private static LevelDbKeyValueService createKv(String dataDir) {
        try {
            return LevelDbKeyValueService.create(new File(dataDir));
        } catch (DBException | IOException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }
}
