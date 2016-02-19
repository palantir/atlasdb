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
package com.palantir.atlasdb.keyvalue.partition;

import java.util.List;

import org.immutables.value.Value;
import org.immutables.value.Value.Check;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;

@AutoService(KeyValueServiceConfig.class)
@JsonTypeName(StaticPartitionedKeyValueConfiguration.TYPE)
@JsonDeserialize(as = ImmutableStaticPartitionedKeyValueConfiguration.class)
@JsonSerialize(as = ImmutableStaticPartitionedKeyValueConfiguration.class)
@Value.Immutable
public abstract class StaticPartitionedKeyValueConfiguration implements KeyValueServiceConfig {
    public static final String TYPE = "static_partitioned";

    @Override
    public final String type() {
        return TYPE;
    }

    public abstract List<String> getKeyValueEndpoints();

    @Check
    protected void check() {
        Preconditions.checkArgument(!getKeyValueEndpoints().isEmpty());
        Preconditions.checkArgument(Sets.newHashSet(getKeyValueEndpoints()).size() == getKeyValueEndpoints().size());
    }
}
