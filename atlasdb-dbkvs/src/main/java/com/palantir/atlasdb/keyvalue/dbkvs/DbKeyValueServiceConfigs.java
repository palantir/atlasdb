/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.dbkvs;

import java.util.Optional;

import com.palantir.nexus.db.pool.config.ConnectionConfig;
import com.palantir.nexus.db.pool.config.ImmutablePostgresConnectionConfig;
import com.palantir.nexus.db.pool.config.PostgresConnectionConfig;
import com.palantir.util.OptionalResolver;

public final class DbKeyValueServiceConfigs {
    private DbKeyValueServiceConfigs() {
        // Utility
    }

    public static DbKeyValueServiceConfig copyWithNamespace(
            DbKeyValueServiceConfig dbKvsConfig,
            Optional<String> namespace) {
        ConnectionConfig connectionConfig = dbKvsConfig.connection();
        if (!(connectionConfig instanceof PostgresConnectionConfig)) {
            // The namespace is not factored in to the underlying connection config.
            // TODO (jkong): Support population of the sid for Oracle? Not sure if it's possible
            return dbKvsConfig;
        }
        ConnectionConfig newConnectionConfig = replacePostgresDbName(
                (PostgresConnectionConfig) connectionConfig, namespace);
        return ImmutableDbKeyValueServiceConfig.builder()
                .from(dbKvsConfig)
                .connection(newConnectionConfig)
                .build();
    }

    private static ConnectionConfig replacePostgresDbName(
            PostgresConnectionConfig connectionConfig,
            Optional<String> namespace) {
        String resolvedNamespace = OptionalResolver.resolve(connectionConfig.rawDbName(), namespace);
        return ImmutablePostgresConnectionConfig.builder()
                .from(connectionConfig)
                .rawDbName(resolvedNamespace)
                .build();
    }
}
