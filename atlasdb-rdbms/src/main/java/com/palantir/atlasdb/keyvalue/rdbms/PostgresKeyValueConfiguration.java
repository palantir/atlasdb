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

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.common.annotation.Immutable;

@Immutable public final class PostgresKeyValueConfiguration implements KeyValueServiceConfig {

    @Nonnull public final String host;
    @Nonnull public final int port;
    @Nonnull public final String db;
    @Nonnull public final String user;
    @Nonnull public final String password;

    @JsonCreator
    public PostgresKeyValueConfiguration(
            @JsonProperty("host") String host,
            @JsonProperty("port") int port,
            @JsonProperty("db") String db,
            @JsonProperty("user") String user,
            @JsonProperty("password") String password) {

        this.host = host;
        this.port = port;
        this.db = db;
        this.user = user;
        this.password = password;
    }

}
