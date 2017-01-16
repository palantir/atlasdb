/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.performance.backend;

import java.net.InetSocketAddress;

import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableDbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutablePostgresDdlConfig;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.nexus.db.pool.config.ImmutableMaskedValue;
import com.palantir.nexus.db.pool.config.ImmutablePostgresConnectionConfig;

public class PostgresKeyValueServiceInstrumentation extends KeyValueServiceInstrumentation {

    public PostgresKeyValueServiceInstrumentation() {
        super(5432, "postgres-docker-compose.yml");
    }

    @Override
    public KeyValueServiceConfig getKeyValueServiceConfig(InetSocketAddress addr) {
        return ImmutableDbKeyValueServiceConfig.builder()
                .ddl(ImmutablePostgresDdlConfig.builder().build())
                .connection(getImmutablePostgresConnectionConfig(addr))
                .build();
    }

    private ImmutablePostgresConnectionConfig getImmutablePostgresConnectionConfig(InetSocketAddress addr) {
        return ImmutablePostgresConnectionConfig.builder()
                .host(addr.getHostString())
                .port(5432)
                .dbName("atlas")
                .dbLogin("palantir")
                .dbPassword(ImmutableMaskedValue.of("palantir"))
                .build();
    }

    @Override
    public boolean canConnect(InetSocketAddress addr) {
        return true;
    }

    @Override
    public String toString() {
        return "POSTGRES";
    }
}
