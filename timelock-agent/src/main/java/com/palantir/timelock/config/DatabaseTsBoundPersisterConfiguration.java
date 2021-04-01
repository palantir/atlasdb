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
package com.palantir.timelock.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableDatabaseTsBoundPersisterConfiguration.class)
@JsonSerialize(as = ImmutableDatabaseTsBoundPersisterConfiguration.class)
@Value.Immutable
public abstract class DatabaseTsBoundPersisterConfiguration implements TsBoundPersisterConfiguration {

    @JsonProperty("key-value-service")
    public abstract KeyValueServiceConfig keyValueServiceConfig();

    @Override
    public boolean isLocationallyIncompatible(TsBoundPersisterConfiguration other) {
        // More can be done e.g. to mitigate the impact of a KVS migration: we can check that database names or paths
        // agree, for instance. But this gives us a starting point, nonetheless.
        return !(other instanceof DatabaseTsBoundPersisterConfiguration);
    }

    /*
     * "relational" is hard-coded from DbKeyValueServiceConfig
     * to avoid taking a compile time dependency on atlasdb-dbkvs
     */
    @Value.Check
    public void check() {
        String kvsType = keyValueServiceConfig().type();
        Preconditions.checkArgument(
                kvsType.equals("relational") || kvsType.equals("memory"),
                "Only InMemory/Dbkvs is a supported for TimeLock's database persister. Found %s.",
                kvsType);
    }
}
