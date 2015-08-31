package com.palantir.atlasdb.keyvalue.partition.endpoint;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.partition.map.PartitionMapService;
import com.palantir.atlasdb.keyvalue.remoting.RemotingKeyValueService;
import com.palantir.atlasdb.keyvalue.remoting.RemotingPartitionMapService;

public class SimpleKeyValueEndpoint implements KeyValueEndpoint {
    transient KeyValueService kvs;
    final transient PartitionMapService pms;
    @JsonProperty("kvsUri") final String kvsUri;
    @JsonProperty("pmsUri") final String pmsUri;

    @JsonCreator
    public SimpleKeyValueEndpoint(@JsonProperty("kvsUri") String kvsUri,
                                  @JsonProperty("pmsUri") String pmsUri) {
        this.kvsUri = Preconditions.checkNotNull(kvsUri);
        this.pmsUri = Preconditions.checkNotNull(pmsUri);
        this.pms = RemotingPartitionMapService.createClientSide(pmsUri);
    }

    @Override
    public KeyValueService keyValueService() {
        return Preconditions.checkNotNull(kvs);
    }

    @Override
    public PartitionMapService partitionMapService() {
        return pms;
    }

    @Override
    public void build(Supplier<Long> clientVersionSupplier) {
        // TODO:
//        Preconditions.checkState(kvs == null);
        kvs = RemotingKeyValueService.createClientSide(kvsUri, clientVersionSupplier);
        kvs = RemotingKeyValueService.FillInUrlProxy.newFillInUrlProxy(kvs, pmsUri);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((kvsUri == null) ? 0 : kvsUri.hashCode());
        result = prime * result + ((pmsUri == null) ? 0 : pmsUri.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SimpleKeyValueEndpoint other = (SimpleKeyValueEndpoint) obj;
        if (kvsUri == null) {
            if (other.kvsUri != null)
                return false;
        } else if (!kvsUri.equals(other.kvsUri))
            return false;
        if (pmsUri == null) {
            if (other.pmsUri != null)
                return false;
        } else if (!pmsUri.equals(other.pmsUri))
            return false;
        return true;
    }

}
