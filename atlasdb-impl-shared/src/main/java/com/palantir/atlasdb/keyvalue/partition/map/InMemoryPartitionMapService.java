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
package com.palantir.atlasdb.keyvalue.partition.map;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;

public class InMemoryPartitionMapService implements PartitionMapService {

    private DynamicPartitionMap partitionMap;

    private InMemoryPartitionMapService(DynamicPartitionMap partitionMap) {
        this.partitionMap = partitionMap;
    }

    public static InMemoryPartitionMapService create(DynamicPartitionMap partitionMap) {
        return new InMemoryPartitionMapService(Preconditions.checkNotNull(partitionMap));
    }

    /**
     * You must store a map using {@link #updateMap(DynamicPartitionMap)} before
     * calling {@link #getMap()} or {@link #getMapVersion()}.
     *
     */
    public static InMemoryPartitionMapService createEmpty() {
        return new InMemoryPartitionMapService(null);
    }

    @Override
    public DynamicPartitionMap getMap() {
        return Preconditions.checkNotNull(partitionMap);
    }

    @Override
    public long getMapVersion() {
        return partitionMap.getVersion();
    }

    @Override
    public void updateMap(DynamicPartitionMap partitionMap) {
        this.partitionMap = Preconditions.checkNotNull(partitionMap);
    }

}
