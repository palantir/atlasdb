/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.timelock.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.paxos.SqliteConnectionConfig;
import com.palantir.paxos.SqliteConnections;
import java.io.File;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableSqlitePaxosPersistenceConfiguration.class)
@JsonSerialize(as = ImmutableSqlitePaxosPersistenceConfiguration.class)
@Value.Immutable
@SuppressWarnings("ClassInitializationDeadlock")
public interface SqlitePaxosPersistenceConfiguration {
    @Deprecated
    SqlitePaxosPersistenceConfiguration DEFAULT = ImmutableSqlitePaxosPersistenceConfiguration.builder()
            .dataDirectory(new File("var/data/sqlitePaxos"))
            .build();

    /**
     * Indicates where the SQLite paxos state log files should be stored.
     * Should not conflict with any other data directories that may be used by TimeLock to store Paxos state logs.
     */
    @JsonProperty("data-directory")
    File dataDirectory();

    @JsonProperty("sqlite-connection-config")
    @Value.Default
    default SqliteConnectionConfig connectionConfig() {
        return SqliteConnections.DEFAULT_SQLITE_CONNECTION_CONFIG;
    }
}
