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

package com.palantir.timelock.config;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;

@JsonDeserialize(as = ImmutableDatabaseTsBoundPersisterConfiguration.class)
@JsonSerialize(as = ImmutableDatabaseTsBoundPersisterConfiguration.class)
@Value.Immutable
public abstract class DatabaseTsBoundPersisterConfiguration implements TsBoundPersisterConfiguration {

    @JsonProperty("key-value-service")
    public abstract KeyValueServiceConfig keyValueServiceConfig();

    /*
     * "cassandra" is hard-coded from CassandraKeyValueServiceConfig.java
     * to avoid taking a compile time dependency on atlasdb-cassandra
     */
    @Value.Check
    public void check() {
        Preconditions.checkArgument(!keyValueServiceConfig().type().equals("cassandra"),
                "Cassandra is not a supported KeyValueService for TimeLock's database persister");
    }
}
