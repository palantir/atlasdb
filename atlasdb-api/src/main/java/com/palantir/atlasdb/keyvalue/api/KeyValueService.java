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
package com.palantir.atlasdb.keyvalue.api;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Multimap;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.annotation.NonIdempotent;
import com.palantir.common.base.ClosableIterator;
import com.palantir.util.paging.BasicResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

/**
 * A service which stores key-value pairs.
 */
public interface KeyValueService extends Closeable {
    /**
     * Performs any initialization that must be done on a fresh instance of the key-value store,
     * such as creating the metadata table.
     *
     * This method should be called when the key-value store is first created. Further calls in the
     * lifetime of the key-value store should be silently ignored.
     */
    void initializeFromFreshInstance();

    /**
     * Performs non-destructive cleanup when the KVS is no longer needed.
     */
    void close();

    /**
     * Performs any cleanup when clearing the database. This method may delete data irrecoverably.
     */
    void teardown();

    /**
     * Gets all key value services this key value service delegates to directly.
     * <p>
     * This can be used to decompose a complex key value service using table splits, tiers,
     * or other delegating operations into its subcomponents.
     */
    Collection<? extends KeyValueService> getDelegates();

    /**
     * Gets values from the key-value store.
     *
     * @param tableName the name of the table to retrieve values from.
     * @param rows set containing the rows to retrieve values for.
     * @param columnSelection specifies the set of columns to fetch.
     * @param timestamp specifies the maximum timestamp (exclusive) at which to
     *        retrieve each rows's value.
     * @return map of retrieved values. Values which do not exist (either
     *         because they were deleted or never created in the first place)
     *         are simply not returned.
     * @throws IllegalArgumentException if any of the requests were invalid
     *         (e.g., attempting to retrieve values from a non-existent table).
     */
    @Idempotent
    Map<Cell, Value> getRows(String tableName, Iterable<byte[]> rows,
                             ColumnSelection columnSelection,long timestamp);

    /**
     * Gets values from the key-value store.
     *
     * @param tableName the name of the table to retrieve values from.
     * @param timestampByCell specifies, for each row, the maximum timestamp (exclusive) at which to
     *        retrieve that rows's value.
     * @return map of retrieved values. Values which do not exist (either
     *         because they were deleted or never created in the first place)
     *         are simply not returned.
     * @throws IllegalArgumentException if any of the requests were invalid
     *         (e.g., attempting to retrieve values from a non-existent table).
     */
    @Idempotent
    Map<Cell, Value> get(String tableName, Map<Cell, Long> timestampByCell);

    /**
     * Gets timestamp values from the key-value store.
     *
     * @param tableName the name of the table to retrieve values from.
     * @param timestampByCell map containing the cells to retrieve timestamps for. The map
     *        specifies, for each key, the maximum timestamp (exclusive) at which to
     *        retrieve that key's value.
     * @return map of retrieved values. cells which do not exist (either
     *         because they were deleted or never created in the first place)
     *         are simply not returned.
     * @throws IllegalArgumentException if any of the requests were invalid
     *         (e.g., attempting to retrieve values from a non-existent table).
     */
    @Idempotent
    Map<Cell, Long> getLatestTimestamps(String tableName, Map<Cell, Long> timestampByCell);

    /**
     * Puts values into the key-value store. This call <i>does not</i> guarantee
     * atomicity across cells. On failure, it is possible
     * that some of the requests will have succeeded (without having been rolled
     * back). Similarly, concurrent batched requests may interleave.
     * <p>
     * If the key-value store supports durability, this call guarantees that the
     * requests have successfully been written to disk before returning.
     * <p>
     * This method may be non-idempotent. On some write-once implementations retrying this call may result in failure.
     * Usually the way around this is to bump the timestamp if you wish to retry.
     * <p>
     * Putting a null value is the same as putting the empty byte[].  If you want to delete a value
     * try {@link #delete(String, Multimap)}.
     *
     * This method should NEVER write a value if timestamp &lt;= gc_ts. This means that the
     * checkAndAct must be atomic.
     *
     * May throw KeyAlreadyExistsException, but this is not guaranteed even if the key exists - see {@link putUnlessExists}.
     *
     * @param tableName the name of the table to put values into.
     * @param values map containing the key-value entries to put.
     * @param timestamp must be non-negative and not equal to {@link Long#MAX_VALUE}
     */
    void put(String tableName, Map<Cell, byte[]> values, long timestamp) throws KeyAlreadyExistsException;

    /**
     * Puts values into the key-value store. This call <i>does not</i> guarantee
     * atomicity across cells. On failure, it is possible
     * that some of the requests will have succeeded (without having been rolled
     * back). Similarly, concurrent batched requests may interleave.
     * <p>
     * If the key-value store supports durability, this call guarantees that the
     * requests have successfully been written to disk before returning.
     * <p>
     * This method may be non-idempotent. On some write-once implementations retrying this call may result in failure.
     * Usually the way around this is to bump the timestamp if you wish to retry.
     * <p>
     * Putting a null value is the same as putting the empty byte[].  If you want to delete a value
     * try {@link #delete(String, Multimap)}.
     *
     * This method should NEVER write a value if timestamp &lt;= gc_ts. This means that the
     * checkAndAct must be atomic.
     *
     * May throw KeyAlreadyExistsException, but this is not guaranteed even if the key exists - see {@link #putUnlessExists(String, Map)}.
     *
     * @param valuesByTable map containing the key-value entries to put by table.
     * @param timestamp must be non-negative and not equal to {@link Long#MAX_VALUE}
     */
    void multiPut(Map<String, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp) throws KeyAlreadyExistsException;

    /**
     * Puts values into the key-value store with individually specified timestamps.
     * This call <i>does not</i> guarantee atomicity across cells. On failure, it is possible
     * that some of the requests will have succeeded (without having been rolled
     * back). Similarly, concurrent batched requests may interleave.
     * <p>
     * If the key-value store supports durability, this call guarantees that the
     * requests have successfully been written to disk before returning.
     * <p>
     * This method may be non-idempotent. On some write-once implementations retrying this
     * call may result in failure. The way around this is to delete and retry.
     * <p>
     * Putting a null value is the same as putting the empty byte[].  If you want to delete a value
     * try {@link #delete(String, Multimap)}.
     *
     * May throw KeyAlreadyExistsException, but this is not guaranteed even if the key exists - see {@link #putUnlessExists(String, Map)}.
     *
     * @param tableName the name of the table to put values into.
     * @param cellValues map containing the key-value entries to put with
     *               non-negative timestamps less than {@link Long#MAX_VALUE}.
     */
    @NonIdempotent
    void putWithTimestamps(String tableName, Multimap<Cell, Value> cellValues) throws KeyAlreadyExistsException;

    /**
     * Puts values into the key-value store. This call <i>does not</i> guarantee
     * atomicity across cells. On failure, it is possible
     * that some of the requests will have succeeded (without having been rolled
     * back). Similarly, concurrent batched requests may interleave.  However, concurrent writes to the same
     * Cell will not both report success.  One of them will throw {@link KeyAlreadyExistsException}.
     * <p>
     * A single Cell will only ever take on one value.
     * <p>
     * If the call completes successfully then you know that your value was written and no other value was written
     * first.  If a {@link KeyAlreadyExistsException} is thrown it may be because the underlying call did a retry and
     * your value was actually put successfully.  It is recommended that you check the stored value to account for this case.
     * <p>
     * Retry should be done by the underlying implementation to ensure that other exceptions besides
     * {@link KeyAlreadyExistsException} are not thrown spuriously.
     *
     * @param tableName the name of the table to put values into.
     * @param values map containing the key-value entries to put.
     * @throws KeyAlreadyExistsException If you are putting a Cell with the same timestamp as
     *                                      one that already exists.
     */
    void putUnlessExists(String tableName, Map<Cell, byte[]> values) throws KeyAlreadyExistsException;

    /**
     * Deletes values from the key-value store.
     * <p>
     * This call <i>does not</i> guarantee atomicity for deletes across (Cell, ts) pairs. However it
     * MUST be implemented where timestamps are deleted in increasing order for each Cell. This
     * means that if there is a request to delete (c, 1) and (c, 2) then the system will never be in
     * a state where (c, 2) was successfully deleted but (c, 1) still remains. It is possible that
     * if there is a failure, then some of the cells may have succeeded. Similarly, concurrent
     * batched requests may interleave.
     * <p>
     * If the key-value store supports durability, this call guarantees that the requests have
     * successfully been written to disk before returning.
     * <p>
     * If a key value store supports garbage collection, then a call to delete should mean the value
     * will not be read in the future. If GC isn't supported, then delete can be written to have a
     * best effort attempt to delete the values.
     * <p>
     * Some systems may require more nodes to be up to ensure that a delete is successful. If this
     * is the case then this method may throw if the delete can't be completed on all nodes.
     *
     * @param tableName the name of the table to delete values from.
     * @param keys map containing the keys to delete values for; the map should specify, for each
     *        key, the timestamp of the value to delete.
     */
    @Idempotent
    void delete(String tableName, Multimap<Cell, Long> keys);

    /**
     * Truncate a table in the key-value store.
     * <p>
     * This is preferred to dropping and re-adding a table, as live schema changes can
     * be a complicated topic for distributed databases.
     *
     * @param tableName the name of the table to truncate.
     *
     * @throws InsufficientConsistencyException if not all hosts respond successfully
     */
    @Idempotent
    void truncateTable(String tableName) throws InsufficientConsistencyException;

    /**
     * Truncate tables in the key-value store.
     * <p>
     * This can be slightly faster than truncating a single table.
     *
     * @param tableNames the name of the tables to truncate.
     *
     * @throws InsufficientConsistencyException if not all hosts respond successfully
     */
    @Idempotent
    void truncateTables(Set<String> tableNames) throws InsufficientConsistencyException;

    /**
     * For each row in the specified range, returns the most recent version strictly before
     * timestamp.
     *
     * Remember to close any {@link ClosableIterator}s you get in a finally block.
     *
     * @param tableName
     * @param rangeRequest the range to load.
     * @param timestamp specifies the maximum timestamp (exclusive) at which to retrieve each rows's
     *        value.
     */
    @Idempotent
    ClosableIterator<RowResult<Value>> getRange(String tableName,
                                                RangeRequest rangeRequest,
                                                long timestamp);

    /**
     * For each row in the specified range, returns all versions strictly before
     * timestamp.
     *
     * Remember to close any {@link ClosableIterator}s you get in a finally block.
     *
     * @param tableName
     * @param rangeRequest the range to load.
     * @param timestamp specifies the maximum timestamp (exclusive) at which to
     *        retrieve each rows's values.
     */
    @Idempotent
    ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(String tableName,
                                                                RangeRequest rangeRequest,
                                                                long timestamp);

    /**
     * Gets timestamp values from the key-value store. For each row, this returns all associated
     * timestamps &lt; given_ts.
     * <p>
     * This method has stronger consistency guarantees than regular read requests. This must return
     * all timestamps stored anywhere in the system. An example of where this could happen is if we
     * use a system with QUORUM reads and writes. Under normal operations reads only need to talk to
     * a Quorum of hosts. However this call MUST be implemented by talking to ALL the nodes where a
     * value could be stored.
     *
     * @param tableName the name of the table to read from.
     * @param rangeRequest the range to load.
     * @param timestamp the maximum timestamp to load.
     *
     * @throws InsufficientConsistencyException if not all hosts respond successfully
     */
    @Idempotent
    ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(String tableName,
                                                                RangeRequest rangeRequest,
                                                                long timestamp) throws InsufficientConsistencyException;

    /**
     * For each range passed in the result will have the first page of results for that range.
     * <p>
     * The page size for each range is dictated by the parameter {@link RangeRequest#getBatchHint()}.
     * If no batch size hint is specified for a range, then it will just get the first row in
     * that range.
     * <p>
     * It is possible that the results may be empty if the first cells after the start of the range
     * all have timestamps greater than the requested timestamp. In this case
     * {@link TokenBackedBasicResultsPage#moreResultsAvailable()} will return true and the token
     * for the next page will be set.
     * <p>
     * It may be possible to get back a result with {@link BasicResultsPage#moreResultsAvailable()}
     * set to true when there aren't more left.  The next call will return zero results and have
     * moreResultsAvailable set to false.
     */
    @Idempotent
    Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(String tableName,
            Iterable<RangeRequest> rangeRequests,
            long timestamp);

    ////////////////////////////////////////////////////////////
    // TABLE CREATION AND METADATA
    ////////////////////////////////////////////////////////////

    @Idempotent
    void dropTable(String tableName) throws InsufficientConsistencyException;

    /**
     * Creates a table with the specified name. If the table already exists, no action is performed
     * (the table is left in its current state).
     *
     * @param tableName
     * @param maxValueSizeInBytes This may be used by the key value store to
     *        throw if a value is too big. It may also be used by the store as a
     *        hint for small values so we can cache them more effectively in memory.
     */
    @Idempotent
    void createTable(String tableName, int maxValueSizeInBytes) throws InsufficientConsistencyException;

    /**
     * Creates many tables in idempotent fashion. If you are making many tables at once,
     * use this call as the implementation can be much faster on some distributed KVSs.
     *
     * @param tableNamesToMaxValueSizeInBytes This may be used by the key value store to
     *        throw if a value is too big. It may also be used by the store as a
     *        hint for small values so we can cache them more effectively in memory.
     */
    @Idempotent
    void createTables(Map<String, Integer> tableNamesToMaxValueSizeInBytes) throws InsufficientConsistencyException;

    @Idempotent
    Set<String> getAllTableNames();

    @Idempotent
    byte[] getMetadataForTable(String tableName);

    @Idempotent
    Map<String, byte[]> getMetadataForTables();

    @Idempotent
    void putMetadataForTable(String tableName, byte[] metadata);

    @Idempotent
    void putMetadataForTables(final Map<String, byte[]> tableNameToMetadata);

    ////////////////////////////////////////////////////////////
    // METHODS TO SUPPORT GARBAGE COLLECTION
    ////////////////////////////////////////////////////////////

    /**
     * Adds a value with timestamp = Value.INVALID_VALUE_TIMESTAMP to each of the given cells. If
     * a value already exists at that time stamp, nothing is written for that cell.
     */
    @Idempotent
    void addGarbageCollectionSentinelValues(String tableName, Set<Cell> cells);

    /**
     * Gets timestamp values from the key-value store. For each cell, this returns all associated
     * timestamps &lt; given_ts.
     * <p>
     * This method has stronger consistency guarantees than regular read requests. This must return
     * all timestamps stored anywhere in the system. An example of where this could happen is if we
     * use a system with QUORUM reads and writes. Under normal operations reads only need to talk to
     * a Quorum of hosts. However this call MUST be implemented by talking to ALL the nodes where a
     * value could be stored.
     *
     * @param tableName the name of the table to delete values from.
     * @param cells set containg cells to retrieve timestamps for.
     * @param timestamp maximum timestamp to get (exclusive)
     * @return multimap of timestamps by cell
     *
     * @throws InsufficientConsistencyException if not all hosts respond successfully
     */
    @Idempotent
    Multimap<Cell, Long> getAllTimestamps(String tableName, Set<Cell> cells, long timestamp) throws InsufficientConsistencyException;

    /**
     * Does whatever can be done to compact or cleanup a table. Intended to be called after many
     * deletions are performed.
     *
     * This call must be implemented so that it completes synchronously.
     */
    void compactInternally(String tableName);
}
