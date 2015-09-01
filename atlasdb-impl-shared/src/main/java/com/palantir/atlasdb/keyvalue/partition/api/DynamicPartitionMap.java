package com.palantir.atlasdb.keyvalue.partition.api;

import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;

/**
 * Dynamic means that the actual partition map can
 * be modified during its life span.
 *
 * @author htarasiuk
 *
 */
public interface DynamicPartitionMap extends PartitionMap {

    /**
     * Add additional endpoint to the existing partition map.
     *
     * The preconditions for this operation are implementation-defined.
     * It will return <code>false</code> if the preconditions were not
     * met and the request was rejected. It will return <code>true</code>
     * if the request was accepted.
     *
     * The request might be completed asynchronously, after returning
     * from this method.
     *
     * @param key
     * @param kvs
     * @param rack
     * @return
     */
    boolean addEndpoint(byte[] key, KeyValueEndpoint kvs, String rack);

    /**
     * Remove existing endpoint from the partition map.
     *
     * The preconditions for this operation are implementation-defined.
     * It will return <code>false</code> if the preconditions were not
     * met and the request was rejected. It will return <code>true</code>
     * if the request was accepted.
     *
     * The request might be completed asynchronosuly, after returning
     * from this method.
     *
     * @param key
     * @param kvs
     * @param rack
     * @return
     */
    boolean removeEndpoint(byte[] key);

    /**
     * In order to ensure consistency across multiple clients and endpoint, the
     * partition map must be versioned.
     *
     * The initial version MUST be 0L!
     *
     * @return Current version of the map.
     */
    long getVersion();
}
