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

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.partition.PartitionedKeyValueConstants;
import com.palantir.atlasdb.keyvalue.partition.map.PartitionMapService;
import com.palantir.atlasdb.keyvalue.remoting.RemotingKeyValueService;
import com.palantir.atlasdb.keyvalue.remoting.RemotingPartitionMapService;
import com.palantir.atlasdb.keyvalue.remoting.proxy.FillInUrlProxy;

/**
 * This is a serializable version of KeyValueEndpoint.
 * It serializes to service URIs and creates Feign proxies
 * on deserialization.
 *
 * You should call build() method each time after deserialization
 * or when assigning to a different partition map, so that the
 * endpoint knows its local partition map version.
 *
 * @author htarasiuk
 *
 */
public final class SimpleKeyValueEndpoint implements KeyValueEndpoint {

    transient KeyValueService kvs;
    final transient PartitionMapService pms;
    @JsonProperty("kvsUri") final String kvsUri;
    @JsonProperty("pmsUri") final String pmsUri;
    @JsonProperty("rack") final String rack;

    private SimpleKeyValueEndpoint(String kvsUri, String pmsUri, String rack) {
        this.kvsUri = Preconditions.checkNotNull(kvsUri, "kvsUri cannot be null");
        this.pmsUri = Preconditions.checkNotNull(pmsUri, "pmgUri cannot be null");
        this.pms = RemotingPartitionMapService.createClientSide(pmsUri);
        this.rack = KeyValueEndpoints.makeUniqueRackIfNoneSpecified(rack);
    }

    /**
     * Creates a new {@link SimpleKeyValueEndpoint}.
     *
     * @param rack Use <tt>PartitionedKeyValueConstants.NO_RACK</tt> if you want
     * to have a unique rack id created. A convenience method {@link #create(String, String)}
     * is also available.
     */
    @JsonCreator
    public static SimpleKeyValueEndpoint create(@JsonProperty("kvsUri") String kvsUri,
                                                @JsonProperty("pmsUri") String pmsUri,
                                                @JsonProperty("rack") String rack) {
        return new SimpleKeyValueEndpoint(kvsUri, pmsUri, rack);
    }

    /**
     * Same as {@link #create(String, String, String)} but will automatically
     * create a unique rack id for this enpoint.
     */
    public static SimpleKeyValueEndpoint create(String kvsUri, String pmsUri) {
        return create(kvsUri, pmsUri, PartitionedKeyValueConstants.NO_RACK);
    }

    @Override
    public KeyValueService keyValueService() {
        return Preconditions.checkNotNull(kvs, "kvs is not set");
    }

    @Override
    public PartitionMapService partitionMapService() {
        return pms;
    }

    @Override
    public String rack() {
        return rack;
    }

    public String partitionMapServiceUri() {
        return pmsUri;
    }

    public String keyValueServiceUri() {
        return kvsUri;
    }

    @Override
    public void registerPartitionMapVersion(Supplier<Long> clientVersionSupplier) {
        Preconditions.checkState(kvs == null);
        kvs = RemotingKeyValueService.createClientSide(kvsUri, clientVersionSupplier);
        kvs = FillInUrlProxy.newFillInUrlProxy(kvs, pmsUri);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SimpleKeyValueEndpoint that = (SimpleKeyValueEndpoint) obj;
        return Objects.equals(kvsUri, that.kvsUri)
                && Objects.equals(pmsUri, that.pmsUri)
                && Objects.equals(rack, that.rack);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kvsUri, pmsUri, rack);
    }

    @Override
    public String toString() {
        return "SimpleKeyValueEndpoint [kvsUri=" + kvsUri + ", pmsUri=" + pmsUri
                + ", rack=" + rack + "]";
    }

}
