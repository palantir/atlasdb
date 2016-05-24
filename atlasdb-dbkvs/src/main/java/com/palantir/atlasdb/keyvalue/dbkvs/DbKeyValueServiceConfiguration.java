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
package com.palantir.atlasdb.keyvalue.dbkvs;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;

@JsonDeserialize(as = ImmutableDbKeyValueServiceConfiguration.class)
@JsonSerialize(as = ImmutableDbKeyValueServiceConfiguration.class)
@JsonTypeName(DbKeyValueServiceConfiguration.TYPE)
@Value.Immutable
public abstract class DbKeyValueServiceConfiguration implements KeyValueServiceConfig {

    public static final String TYPE = "relational";

    @Value.Default
    public boolean oracleEnableEeFeatures() {
        return false;
    }

    @Value.Default
    public int getRangeConcurrency() {
        return 64;
    }

    @Value.Default
    public int postgresQueryPoolSize() {
        return 64;
    }

    @Value.Default
    public int postgresQueryBatchSize() {
        return 256;
    }

    @Override
    public final String type() {
        return TYPE;
    }

}
