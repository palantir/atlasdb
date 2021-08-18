/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.paxos;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.zaxxer.hikari.HikariConfig;
import java.util.concurrent.TimeUnit;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableSqliteConnectionConfig.class)
@JsonDeserialize(as = ImmutableSqliteConnectionConfig.class)
public interface SqliteConnectionConfig {
    @Value.Default
    default int maximumPoolSize() {
        return 1;
    }

    /**
     * how long to wait for a connection to be opened.
     */
    @Value.Default
    default int connectionTimeoutSeconds() {
        return 30;
    }

    @JsonIgnore
    @Value.Lazy
    default HikariConfig getHikariConfig() {
        HikariConfig config = new HikariConfig();

        config.setMaximumPoolSize(maximumPoolSize());
        config.setConnectionTimeout(TimeUnit.SECONDS.toMillis(connectionTimeoutSeconds()));

        return config;
    }
}
