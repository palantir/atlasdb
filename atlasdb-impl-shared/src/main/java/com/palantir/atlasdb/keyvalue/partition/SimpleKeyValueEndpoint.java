package com.palantir.atlasdb.keyvalue.partition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.remoting.RemotingKeyValueService;
import com.palantir.atlasdb.keyvalue.remoting.RemotingPartitionMapService;

public class SimpleKeyValueEndpoint implements KeyValueEndpoint {

    final KeyValueService kvs;
    final PartitionMapService pms;
    @JsonProperty("kvsUri") final String kvsUri;
    @JsonProperty("pmsUri") final String pmsUri;

    @JsonCreator
    public SimpleKeyValueEndpoint(@JsonProperty("kvsUri") String kvsUri,
                                  @JsonProperty("pmsUri") String pmsUri) {
        this.kvsUri = Preconditions.checkNotNull(kvsUri);
        this.pmsUri = Preconditions.checkNotNull(pmsUri);
        this.kvs = RemotingKeyValueService.createClientSide(kvsUri);
        this.pms = RemotingPartitionMapService.createClientSide(pmsUri);
    }

    @Override
    public KeyValueService keyValueService() {
        return kvs;
    }

    @Override
    public PartitionMapService partitionMapService() {
        return pms;
    }
}
