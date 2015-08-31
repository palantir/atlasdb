package com.palantir.atlasdb.keyvalue.partition.api;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;

public interface DynamicPartitionMap extends PartitionMap {

    /**
     *
     * @param key
     * @param kvs
     * @param rack
     * @return True if the operation was started. False if the operation
     * was rejected (eg. due to another operation being in progress).
     */
    boolean addEndpoint(byte[] key, KeyValueService kvs, String rack);

    /**
     *
     * @param key
     * @param kvs
     * @param rack
     * @return True if the operation was started. False if the operation
     * was rejected (eg. due to antoher operation bein in progress).
     */
    boolean removeEndpoint(byte[] key, KeyValueService kvs, String rack);

    /**
     * The initial version MUST be 0L!
     * @return Curent version of the map.
     */
    @Override
    long getVersion();

    /**
     * For testing purposes only. Will be removed soon.
     */
    @Deprecated void setVersion(long version);
}
