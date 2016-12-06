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
package com.palantir.atlasdb.containers;

import com.palantir.atlasdb.keyvalue.dbkvs.DbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.DdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableDbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutablePostgresDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;
import com.palantir.nexus.db.pool.config.ConnectionConfig;
import com.palantir.nexus.db.pool.config.ImmutableMaskedValue;
import com.palantir.nexus.db.pool.config.ImmutablePostgresConnectionConfig;

public class PostgresContainer extends Container {
    public static final DdlConfig DDL_CONFIG = ImmutablePostgresDdlConfig.builder().build();
    public static final ConnectionConfig CONNECTION_CONFIG = ImmutablePostgresConnectionConfig.builder()
            .dbName("atlas")
            .dbLogin("palantir")
            .dbPassword(ImmutableMaskedValue.of("palantir"))
            .host("postgres")
            .port(5432)
            .build();
    public static final DbKeyValueServiceConfig KVS_CONFIG = ImmutableDbKeyValueServiceConfig.builder()
            .ddl(DDL_CONFIG)
            .connection(CONNECTION_CONFIG)
            .build();

    @Override
    public String getDockerComposeFile() {
        return "/docker-compose-postgres.yml";
    }

    @Override
    public SuccessOrFailure isReady(DockerComposeRule rule) {
        return SuccessOrFailure.onResultOf(() -> {
            ConnectionManagerAwareDbKvs.create(KVS_CONFIG);
            return true;
        });
    }
}
