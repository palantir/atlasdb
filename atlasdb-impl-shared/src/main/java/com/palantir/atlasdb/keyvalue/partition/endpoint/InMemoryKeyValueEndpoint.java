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
package com.palantir.atlasdb.keyvalue.partition.endpoint;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.partition.PartitionedKeyValueConstants;
import com.palantir.atlasdb.keyvalue.partition.map.PartitionMapService;

/**
 * This cannot be serialized and should be used for test purposes only.
 * The advantage of this impl is that it can be run with local service
 * instances. You do not need to remote the services over HTTP.
 *
 * @author htarasiuk
 *
 */
public class InMemoryKeyValueEndpoint implements KeyValueEndpoint {

    transient KeyValueService kvs;
    transient final PartitionMapService pms;
    transient final String rack;

    private InMemoryKeyValueEndpoint(KeyValueService kvs, PartitionMapService pms, String rack) {
        this.kvs = Preconditions.checkNotNull(kvs);
        this.pms = Preconditions.checkNotNull(pms);
        this.rack = Preconditions.checkNotNull(rack);
    }

    public static InMemoryKeyValueEndpoint create(KeyValueService kvs, PartitionMapService pms, String rack) {
        return new InMemoryKeyValueEndpoint(kvs, pms, rack);
    }

    public static InMemoryKeyValueEndpoint create(KeyValueService kvs, PartitionMapService pms) {
        return create(kvs, pms, PartitionedKeyValueConstants.NO_RACK);
    }

    @Override
    public KeyValueService keyValueService() {
        return kvs;
    }

    @Override
    public PartitionMapService partitionMapService() {
        return pms;
    }

    @Override
    public String rack() {
        return rack;
    }

    @Override
    public void registerPartitionMapVersion(Supplier<Long> clientVersionSupplier) {
        // No-op
    }

}
