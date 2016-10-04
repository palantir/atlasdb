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
public final class InMemoryKeyValueEndpoint implements KeyValueEndpoint {
    final transient KeyValueService kvs;
    final transient PartitionMapService pms;
    final transient String rack;

    private InMemoryKeyValueEndpoint(KeyValueService kvs, PartitionMapService pms, String rack) {
        this.kvs = Preconditions.checkNotNull(kvs, "kvs cannot be null");
        this.pms = Preconditions.checkNotNull(pms, "pms cannot be null");
        this.rack = KeyValueEndpoints.makeUniqueRackIfNoneSpecified(rack);
    }

    /**
     * Creates an {@link InMemoryKeyValueEndpoint}.
     *
     * @param rack Use <tt>PartitionedKeyValueConstants.NO_RACK</tt> if you want a unique
     * rack name to be created for this endpoint. Also see a convenience method
     * {@link #create(KeyValueService, PartitionMapService)}.
     */
    public static InMemoryKeyValueEndpoint create(KeyValueService kvs, PartitionMapService pms, String rack) {
        return new InMemoryKeyValueEndpoint(kvs, pms, rack);
    }

    /**
     * Will automatically generate a unique rack name
     * for this endpoint.
     * Equivalent to calling {@link #create(KeyValueService, PartitionMapService, String)}
     * with <tt>rack=PartitionedKeyValueConstants.NO_RACK</tt>.
     */
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

    @Override
    public String toString() {
        return "InMemoryKeyValueEndpoint [kvs=" + kvs + ", pms=" + pms
                + ", rack=" + rack + "]";
    }

}
