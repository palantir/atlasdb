package com.palantir.atlasdb.keyvalue.partition.api;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumParameters.QuorumRequestParameters;

/**
 * Dynamic means that the actual partition map can
 * be modified during its life span.
 *
 * @author htarasiuk
 *
 */
public interface DynamicPartitionMap extends PartitionMap {

    /**
     * Add additional endpoint to the partition map.
     *
     * Note that calling this method is not enough to complete the add
     * operation. After this call returns the map should be pushed to enough
     * endpoints and eventually you should start the promotion of the endpoint
     * with <code>promoteAddedEndpoint</code>. Finally the resulting
     * partition map should be pushed once again.
     *
     * The preconditions for this operation are implementation-defined.
     * It will return <code>false</code> if the preconditions were not
     * met and the request was rejected. It will return <code>true</code>
     * if the request was accepted.
     *
     * @param key
     * @param kvs
     * @param rack
     * @return
     */
    boolean addEndpoint(byte[] key, KeyValueEndpoint kve, String rack);

    /**
     * This will do the backfill jobs required for the added endpoint
     * have the full functionality. Afterwards it will update the map
     * to reflect the new status of the endpoint.
     *
     * @param key
     */
    void promoteAddedEndpoint(byte[] key);

    /**
     * Remove existing endpoint from the partition map.
     *
     * The preconditions for this operation are implementation-defined.
     * It will return <code>false</code> if the preconditions were not
     * met and the request was rejected. It will return <code>true</code>
     * if the request was accepted.
     *
     * Note that calling this method is not enough to complete the remove
     * operation. After this call returns the map should be pushed to enough
     * endpoints and eventually you should start the promotion of the endpoint
     * with <code>promoteRemovedEndpoint</code>. Finally the resulting
     * partition map should be pushed once again.
     *
     * @param key
     * @param kvs
     * @param rack
     * @return
     */
    boolean removeEndpoint(byte[] key);

    /**
     * This will do the backfill jobs required for the removal of
     * specified endpoint. Afterwards it will update the map
     * by removing it completely.
     *
     * @param key
     */
    void promoteRemovedEndpoint(byte[] key);

    /**
     * In order to ensure consistency across multiple clients and endpoint, the
     * partition map must be versioned.
     *
     * The initial version MUST be 0L!
     *
     * @return Current version of the map.
     */
    long getVersion();

    /**
     * Pushes this map to all endpoints on the ring.
     * Useful after modifying.
     */
    void pushMapToEndpoints();

    Map<byte[], QuorumRequestParameters> getReadRowsParameters(Iterable<byte[]> rows);
    Map<byte[], QuorumRequestParameters> getWriteRowsParameters(Set<byte[]> rows);
    Map<Cell, QuorumRequestParameters> getReadCellsParameters(Set<Cell> cells);
    Map<Cell, QuorumRequestParameters> getWriteCellsParameters(Set<Cell> cells);
    <T> Map<Entry<Cell, T>, QuorumRequestParameters> getReadEntriesParameters(Map<Cell, T> entries);
    <T> Map<Entry<Cell, T>, QuorumRequestParameters> getWriteEntriesParameters(Map<Cell, T> entries);
    <T> Map<Entry<Cell, T>, QuorumRequestParameters> getReadEntriesParameters(Multimap<Cell, T> entries);
    <T> Map<Entry<Cell, T>, QuorumRequestParameters> getWriteEntriesParameters(Multimap<Cell, T> entries);
}
