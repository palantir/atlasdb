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
 * <p>
 *
 * The usual add-endpoint workflow:
 * <ol>
 *  <li> retry {@link #addEndpoint(byte[], KeyValueEndpoint, String)} until succeeds
 *  <li> retry {@link #backfillAddedEndpoint(byte[])} untill succeeds
 *  <li> retry {@link #promoteAddedEndpoint(byte[])} until succeeds
 * </ol>
 *
 * The usual remove-endpoint workflow:
 * <ol>
 *  <li> retry {@link #removeEndpoint(byte[])} until succeeds
 *  <li> retry {@link #backfillRemovedEndpoint(byte[])} until succeeds
 *  <li> retry {@link #promoteRemovedEndpoint(byte[])} until succeeds
 * </ol>
 *
 * @author htarasiuk
 *
 */
public interface DynamicPartitionMap extends PartitionMap {

    /**
     * Add additional endpoint to the partition map.
     *
     * It must be implemented so that it can be retried if it
     * fails with an exception.
     *
     * It must not be retried after it succeeded (non-idempotent).
     *
     * After completing this call you should backfill the new endpoint
     * with {@link #backfillAddedEndpoint(byte[])}.
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
     * Backfills the new endpoint with data from other endpoints.
     *
     * It must be implemented so that it can be retried if it
     * fails with an exception.
     *
     * It must not be retried after it succeeded (non-idempotent).
     *
     * After completing this call you should promote your new endpoint
     * with {@link #promoteAddedEndpoint(byte[])}.
     *
     * @param key
     */
    void backfillAddedEndpoint(byte[] key);

    /**
     * Promotes a new endpoint after it is backfilled with {@link #backfillAddedEndpoint(byte[])}.
     *
     * It must be implemented so that it can be retried if it
     * fails with an exception.
     *
     * It must not be retried after it succeeded (non-idempotent).
     *
     * After this call completes, the new endpoint is fully functional.
     * You might consider {@link #pushMapToEndpoints()} at this moment.
     *
     * @param key
     */
    void promoteAddedEndpoint(byte[] key);

    /**
     * Remove existing endpoint from the partition map.
     *
     * It must be implemented so that it can be retried if it
     * fails with an exception.
     *
     * It must not be retried after it succeeded (non-idempotent).
     *
     * After completing this call you should backfill the new endpoint
     * with {@link #backfillRemovedEndpoint(byte[])}.
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
    boolean removeEndpoint(byte[] key);

    /**
     * Backfills the new endpoint with data from other endpoints.
     *
     * It must be implemented so that it can be retried if it
     * fails with an exception.
     *
     * It must not be retried after it succeeded (non-idempotent).
     *
     * After completing this call you should promote your new endpoint
     * with {@link #promoteAddedEndpoint(byte[])}.
     *
     * @param key
     *
     */
    void backfillRemovedEndpoint(byte[] key);

    /**
     * Ultimately removes the endpoint from the partition map after
     * other endpoints where backfilled with data from this one
     * using {@link #backfillRemovedEndpoint(byte[])}.
     *
     * It must be implemented so that it can be retried if it
     * fails with an exception.
     *
     * It must not be retried after it succeeded (non-idempotent).
     *
     * After this call completes, the removed endpoint can be taken down.
     * You might consider {@link #pushMapToEndpoints()} at this moment.
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

    // These methods are used to determine quorum parameters for different operations.
    // For example if the original QP=(3, 2, 2) and there is a joining node in the area,
    // the temporary quorum parameters might be (REPF=4, SUCCF=3) instead of (REPF=3, SUCCF=2).
    Map<byte[], QuorumRequestParameters> getReadRowsParameters(Iterable<byte[]> rows);
    Map<byte[], QuorumRequestParameters> getWriteRowsParameters(Set<byte[]> rows);
    Map<Cell, QuorumRequestParameters> getReadCellsParameters(Set<Cell> cells);
    Map<Cell, QuorumRequestParameters> getWriteCellsParameters(Set<Cell> cells);
    <T> Map<Entry<Cell, T>, QuorumRequestParameters> getReadEntriesParameters(Map<Cell, T> entries);
    <T> Map<Entry<Cell, T>, QuorumRequestParameters> getWriteEntriesParameters(Map<Cell, T> entries);
    <T> Map<Entry<Cell, T>, QuorumRequestParameters> getReadEntriesParameters(Multimap<Cell, T> entries);
    <T> Map<Entry<Cell, T>, QuorumRequestParameters> getWriteEntriesParameters(Multimap<Cell, T> entries);
}
