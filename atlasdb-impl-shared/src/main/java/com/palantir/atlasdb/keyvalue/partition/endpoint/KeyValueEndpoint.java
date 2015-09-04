package com.palantir.atlasdb.keyvalue.partition.endpoint;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.google.common.base.Supplier;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.partition.map.PartitionMapService;

/**
 * This class is used to represent an endpoint for the <code>PartitionedKeyValueService</code>.
 * Whenever the keyValueService throws a <code>VersionTooOldException</code>, the partitionMapService
 * shall be used to update local DynamicPartitionMap instance.
 *
 * @author htarasiuk
 *
 */
@JsonTypeInfo(use=Id.CLASS, property="@class")
public interface KeyValueEndpoint {
    KeyValueService keyValueService();
    PartitionMapService partitionMapService();

    /**
     * TODO: This should be replaced by a nicer solution eventually.
     * @param clientVersionSupplier
     */
    void registerPartitionMapVersion(Supplier<Long> clientVersionSupplier);
}
