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

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;
import com.palantir.atlasdb.keyvalue.partition.map.InMemoryPartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.map.PartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumParameters;

/**
 * This class is to provide configuration for PartitionedKeyValueService.
 * The only mutable data is partition map services supplied to the constructor.
 *
 * @author htarasiuk
 *
 */
public class PartitionedKeyValueConfiguration {

    public final QuorumParameters quorumParameters;
    public final ImmutableList<PartitionMapService> partitionMapProviders;

    private PartitionedKeyValueConfiguration(QuorumParameters quorumParameters, List<PartitionMapService> partitionMapProviders) {
        this.quorumParameters = quorumParameters;
        this.partitionMapProviders = ImmutableList.copyOf(partitionMapProviders);
    }

    public static PartitionedKeyValueConfiguration of(QuorumParameters quorumParameters, List<PartitionMapService> partitionMapProviders) {
        return new PartitionedKeyValueConfiguration(quorumParameters, partitionMapProviders);
    }

    public static PartitionedKeyValueConfiguration of(QuorumParameters quorumParameters, DynamicPartitionMap partitionMap) {
        return new PartitionedKeyValueConfiguration(quorumParameters,
                ImmutableList.<PartitionMapService> of(InMemoryPartitionMapService.create(partitionMap)));
    }

}
