/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.performance.backend;

import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableDbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableOracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.nexus.db.pool.config.ImmutableMaskedValue;
import com.palantir.nexus.db.pool.config.ImmutableOracleConnectionConfig;
import com.palantir.nexus.db.pool.config.OracleConnectionConfig;
import java.net.InetSocketAddress;
import java.util.Optional;

public class OracleKeyValueServiceInstrumentation extends KeyValueServiceInstrumentation {

    public OracleKeyValueServiceInstrumentation() {
        super(1521, "oracle-docker-compose.yml");
    }

    @Override
    public KeyValueServiceConfig getKeyValueServiceConfig(InetSocketAddress addr) {
        return ImmutableDbKeyValueServiceConfig.builder()
                .ddl(ImmutableOracleDdlConfig.builder()
                        .overflowMigrationState(OverflowMigrationState.FINISHED)
                        .build())
                .connection(getImmutableOracleConnectionConfig(addr))
                .build();
    }

    @Override
    public Optional<KeyValueServiceRuntimeConfig> getKeyValueServiceRuntimeConfig(InetSocketAddress addr) {
        return Optional.empty();
    }

    private ImmutableOracleConnectionConfig getImmutableOracleConnectionConfig(InetSocketAddress addr) {
        return new OracleConnectionConfig.Builder()
                .host(addr.getHostString())
                .port(addr.getPort())
                .sid("palantir")
                .dbLogin("palantir")
                .dbPassword(ImmutableMaskedValue.of("7_SeeingStones_7"))
                .build();
    }

    @Override
    public boolean canConnect(InetSocketAddress addr) {
        return true;
    }

    @Override
    public String toString() {
        return "ORACLE";
    }
}
