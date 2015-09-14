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
package com.palantir.atlasdb.keyvalue.rdbms;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampService;

public class PostgresAtlasDbFactory implements AtlasDbFactory<PostgresKeyValueService> {

    @Override
    public String getType() {
        return "postgres";
    }

    @Override
    public PostgresKeyValueService createRawKeyValueService(JsonNode config)
            throws IOException {
        return PostgresKeyValueService.create(createConfig(config.get("postgresConfig")));
    }

    @Override
    public TimestampService createTimestampService(
            PostgresKeyValueService rawKvs) {
        return new InMemoryTimestampService();
    }

    private PostgresKeyValueConfiguration createConfig(JsonNode node) {
        String host = node.get("host").asText();
        int port = node.get("port").asInt();
        String db = node.get("db").asText();
        String user = node.get("user").asText();
        String password = node.get("password").asText();

        return new PostgresKeyValueConfiguration(host, port, db, user, password);
    }

}
