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
package com.palantir.atlasdb.keyvalue.partition;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumParameters;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;

/**
 * This class is to provide configuration for PartitionedKeyValueService.
 * The only mutable data is partition map services supplied to the constructor.
 *
 * @author htarasiuk
 *
 */
@JsonTypeName(PartitionedKeyValueConfiguration.TYPE)
@Value.Immutable
public abstract class PartitionedKeyValueConfiguration implements KeyValueServiceConfig {
    public static final String TYPE = "partitioned";

    @Override
    public final String type() {
        return TYPE;
    }

    public abstract QuorumParameters getQuorumParameters();
    public abstract ImmutableList<String> getPartitionMapProviders();
    public abstract int getPartitionMapProvidersReadFactor();

}
