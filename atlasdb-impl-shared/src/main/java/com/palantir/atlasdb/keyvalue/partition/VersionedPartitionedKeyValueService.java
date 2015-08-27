package com.palantir.atlasdb.keyvalue.partition;

import java.util.NavigableMap;
import java.util.concurrent.ExecutorService;

import com.palantir.atlasdb.keyvalue.partition.api.PartitionMap;

/**
 * It provides the PartitionMap service (through VersionedPartitionMap)
 * in addition to the usual KeyValueService.
 *
 * @author htarasiuk
 *
 */
public class VersionedPartitionedKeyValueService extends PartitionedKeyValueService {

    private final VersionedPartitionMap vpm;

    protected VersionedPartitionedKeyValueService(NavigableMap<byte[], KeyValueEndpoint> ring,
                                                  ExecutorService executor,
                                                  QuorumParameters quorumParameters) {
        super(executor, quorumParameters);
        vpm = VersionedPartitionMap.of(1L, new DynamicPartitionMapImpl(quorumParameters, ring));
    }

    public static VersionedPartitionedKeyValueService create(NavigableMap<byte[], KeyValueEndpoint> ring,
                                                      ExecutorService executor,
                                                      QuorumParameters quorumParameters) {
        return new VersionedPartitionedKeyValueService(ring, executor, quorumParameters);
    }

    @Override
    protected PartitionMap getPartitionMap() {
        return vpm.getPartitionMap();
    }

    public VersionedPartitionMap getVersionedPartitionMap() {
        vpm.getPartitionMapService();
        return vpm;
    }

}
