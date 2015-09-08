package com.palantir.atlasdb.keyvalue.partition;

import java.util.Map;

import com.palantir.atlasdb.keyvalue.partition.endpoint.SimpleKeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumParameters;

public class PartitionedKeyValueConfiguration {

    public final QuorumParameters quorumParameters;
    public final Map<byte[], SimpleKeyValueEndpoint> endpoints;

    public PartitionedKeyValueConfiguration(QuorumParameters quorumParameters, Map<byte[], SimpleKeyValueEndpoint> endpoint) {
        this.quorumParameters = quorumParameters;
        this.endpoints = endpoint;
    }

}
