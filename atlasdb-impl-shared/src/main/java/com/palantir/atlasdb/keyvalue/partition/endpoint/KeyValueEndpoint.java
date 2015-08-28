package com.palantir.atlasdb.keyvalue.partition.endpoint;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.partition.api.PartitionMap;
import com.palantir.atlasdb.keyvalue.partition.map.PartitionMapService;

/**
 * This class is used to represent an endpoint for the <code>PartitionedKeyValueService</code>.
 * Whenever the keyValueService throws a <code>VersionTooOldException</code>, the partitionMapService
 * shall be used to update local DynamicPartitionMap instance.
 * 
 * @author htarasiuk
 *
 */
@JsonDeserialize(as=SimpleKeyValueEndpoint.class)
@JsonSerialize(as=SimpleKeyValueEndpoint.class)
public interface KeyValueEndpoint {
    KeyValueService keyValueService();
    PartitionMapService partitionMapService();

    /**
     * TODO: This should be replaced by a nicer solution eventually.
     * @param clientVersionSupplier
     */
    void build(Supplier<Long> clientVersionSupplier);
}
