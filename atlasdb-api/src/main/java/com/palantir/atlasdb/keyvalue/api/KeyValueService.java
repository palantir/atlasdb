/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.google.common.collect.Multimap;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.annotation.NonIdempotent;
import com.palantir.common.base.ClosableIterator;
import com.palantir.util.paging.BasicResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

/**
 * A service which stores key-value pairs.
 */
@Path("/keyvalue")
public interface KeyValueService extends AutoCloseable {
    /**
     * Performs non-destructive cleanup when the KVS is no longer needed.
     */
    @POST
    @Path("close")
    @Override
    void close();

    /**
     * Gets all key value services this key value service delegates to directly.
     * <p>
     * This can be used to decompose a complex key value service using table splits, tiers,
     * or other delegating operations into its subcomponents.
     */
    @POST
    @Path("get-delegates")
    @Produces(MediaType.APPLICATION_JSON)
    Collection<? extends KeyValueService> getDelegates();

    /**
     * Gets values from the key-value store.
     *
     * @param tableRef the name of the table to retrieve values from.
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
    @POST
    @Path("get-rows")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    Map<Cell, Value> getRows(@QueryParam("tableRef") TableReference tableRef,
                             Iterable<byte[]> rows,
                             @QueryParam("columnSelection") ColumnSelection columnSelection,
                             @QueryParam("timestamp") long timestamp);

    /**
     * Gets values from the key-value store for the specified rows and column range
     * as separate iterators for each row.
     *
     * @param tableRef the name of the table to retrieve values from.
     * @param rows set containing the rows to retrieve values for. Behavior is undefined if {@code rows}
     *        contains duplicates (as defined by {@link java.util.Arrays#equals(byte[], byte[])}).
     * @param batchColumnRangeSelection specifies the column range and the per-row batchSize to fetch.
     * @param timestamp specifies the maximum timestamp (exclusive) at which to retrieve each rows's value.
     * @return map of row names to {@link RowColumnRangeIterator}. Each {@link RowColumnRangeIterator} can iterate over
     *         the values that are spanned by the {@code batchColumnRangeSelection} in increasing order by column name.
     * @throws IllegalArgumentException if {@code rows} contains duplicates.
     */
    @POST
    @Path("get-rows-col-range")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    Map<byte[], RowColumnRangeIterator> getRowsColumnRange(
            @QueryParam("tableRef") TableReference tableRef,
            Iterable<byte[]> rows,
            @QueryParam("batchColumnRangeSelection") BatchColumnRangeSelection batchColumnRangeSelection,
            @QueryParam("timestamp") long timestamp);

    /**
     * Gets values from the key-value store for the specified rows and column range as a single iterator. This method
     * should be at least as performant as
     * {@link #getRowsColumnRange(TableReference, Iterable, BatchColumnRangeSelection, long)}, and may be more
     * performant in some cases.
     *
     * @param tableRef the name of the table to retrieve values from.
     * @param rows set containing the rows to retrieve values for. Behavior is undefined if {@code rows}
     *        contains duplicates (as defined by {@link java.util.Arrays#equals(byte[], byte[])}).
     * @param columnRangeSelection specifies the column range to fetch.
     * @param cellBatchHint specifies the batch size for fetching the values.
     * @param timestamp specifies the maximum timestamp (exclusive) at which to
     *        retrieve each rows's value.
     * @return a {@link RowColumnRangeIterator} that can iterate over all the retrieved values. Results for different
     *         rows are in the same order as they are provided in {@code rows}. All columns for a given row are adjacent
     *         and sorted by increasing column name.
     * @throws IllegalArgumentException if {@code rows} contains duplicates.
     */
    @POST
    @Path("get-rows-col-range-2")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    RowColumnRangeIterator getRowsColumnRange(
            @QueryParam("tableRef") TableReference tableRef,
            Iterable<byte[]> rows,
            @QueryParam("columnRangeSelection") ColumnRangeSelection columnRangeSelection,
            @QueryParam("cellBatchHint") int cellBatchHint,
            @QueryParam("timestamp") long timestamp);

    /**
     * Gets values from the key-value store.
     *
     * @param tableRef the name of the table to retrieve values from.
     * @param timestampByCell specifies, for each row, the maximum timestamp (exclusive) at which to
     *        retrieve that rows's value.
     * @return map of retrieved values. Values which do not exist (either
     *         because they were deleted or never created in the first place)
     *         are simply not returned.
     * @throws IllegalArgumentException if any of the requests were invalid
     *         (e.g., attempting to retrieve values from a non-existent table).
     */
    @POST
    @Path("get")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    Map<Cell, Value> get(@QueryParam("tableRef") TableReference tableRef, Map<Cell, Long> timestampByCell);

    /**
     * Gets timestamp values from the key-value store.
     *
     * @param tableRef the name of the table to retrieve values from.
     * @param timestampByCell map containing the cells to retrieve timestamps for. The map
     *        specifies, for each key, the maximum timestamp (exclusive) at which to
     *        retrieve that key's value.
     * @return map of retrieved values. cells which do not exist (either
     *         because they were deleted or never created in the first place)
     *         are simply not returned.
     * @throws IllegalArgumentException if any of the requests were invalid
     *         (e.g., attempting to retrieve values from a non-existent table).
     */
    @POST
    @Path("get-latest-timestamps")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    Map<Cell, Long> getLatestTimestamps(@QueryParam("tableRef") TableReference tableRef,
                                        Map<Cell, Long> timestampByCell);

    /**
     * Puts values into the key-value store. This call <i>does not</i> guarantee
     * atomicity across cells. On failure, it is possible
     * that some of the requests will have succeeded (without having been rolled
     * back). Similarly, concurrent batched requests may interleave.
     * <p>
     * If the key-value store supports durability, this call guarantees that the
     * requests have successfully been written to disk before returning.
     * <p>
     * Putting a null value is the same as putting the empty byte[].  If you want to delete a value
     * try {@link #delete(TableReference, Multimap)}.
     * <p>
     * May throw KeyAlreadyExistsException, if storing a different value to existing key,
     * but this is not guaranteed even if the key exists - see {@link #putUnlessExists}}.
     * <p>
     * Must not throw KeyAlreadyExistsException when overwriting a cell with the original value (idempotent).
     *  @param tableRef the name of the table to put values into.
     * @param values map containing the key-value entries to put.
     * @param timestamp must be non-negative and not equal to {@link Long#MAX_VALUE}
     */
    @POST
    @Path("put")
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    void put(@QueryParam("tableRef") TableReference tableRef,
             Map<Cell, byte[]> values,
             @QueryParam("timestamp") long timestamp) throws KeyAlreadyExistsException;

    /**
     * Puts values into the key-value store. This call <i>does not</i> guarantee
     * atomicity across cells. On failure, it is possible
     * that some of the requests will have succeeded (without having been rolled
     * back). Similarly, concurrent batched requests may interleave.
     * <p>
     * If the key-value store supports durability, this call guarantees that the
     * requests have successfully been written to disk before returning.
     * <p>
     * Putting a null value is the same as putting the empty byte[].  If you want to delete a value
     * try {@link #delete(TableReference, Multimap)}.
     * <p>
     * May throw KeyAlreadyExistsException, if storing a different value to existing key,
     * but this is not guaranteed even if the key exists - see {@link #putUnlessExists}.
     * <p>
     * Must not throw KeyAlreadyExistsException when overwriting a cell with the original value (idempotent).
     *  @param valuesByTable map containing the key-value entries to put by table.
     * @param timestamp must be non-negative and not equal to {@link Long#MAX_VALUE}
     */
    @POST
    @Path("multi-put")
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable,
                  @QueryParam("timestamp") long timestamp) throws KeyAlreadyExistsException;

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
     * try {@link #delete(TableReference, Multimap)}.
     * <p>
     * May throw KeyAlreadyExistsException, if storing a different value to existing key,
     * but this is not guaranteed even if the key exists - see {@link #putUnlessExists}.
     * <p>
     * Must not throw KeyAlreadyExistsException when overwriting a cell with the original value (idempotent).
     *  @param tableRef the name of the table to put values into.
     * @param cellValues map containing the key-value entries to put with
     *               non-negative timestamps less than {@link Long#MAX_VALUE}.
     */
    @POST
    @Path("put-with-timestamps")
    @Consumes(MediaType.APPLICATION_JSON)
    @NonIdempotent
    @Idempotent
    void putWithTimestamps(@QueryParam("tableRef") TableReference tableRef,
                           Multimap<Cell, Value> cellValues) throws KeyAlreadyExistsException;

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
     * your value was actually put successfully.  It is recommended that you check the stored value to account
     * for this case.
     * <p>
     * Retry should be done by the underlying implementation to ensure that other exceptions besides
     * {@link KeyAlreadyExistsException} are not thrown spuriously.
     *
     * @param tableRef the name of the table to put values into.
     * @param values map containing the key-value entries to put.
     * @throws KeyAlreadyExistsException If you are putting a Cell with the same timestamp as
     *                                      one that already exists.
     */
    @POST
    @Path("put-unless-exists")
    @Consumes(MediaType.APPLICATION_JSON)
    void putUnlessExists(@QueryParam("tableRef") TableReference tableRef,
                         Map<Cell, byte[]> values) throws KeyAlreadyExistsException;

    /**
     * Check whether CAS is supported. This check can go away when Rocks and JDBC KVS's are deleted.
     *
     * @return true iff checkAndSet is supported (for all delegates/tables, if applicable)
     */
    @POST
    @Path("supports-check-and-set")
    @Consumes(MediaType.APPLICATION_JSON)
    boolean supportsCheckAndSet();

    /**
     * Performs a check-and-set into the key-value store.
     * Please see {@link CheckAndSetRequest} for information about how to create this request.
     * <p>
     * Note that this call <i>does not</i> guarantee atomicity across Cells.
     * If you attempt to achieve this guarantee by performing multiple checkAndSet calls in a single transaction,
     * and one of the calls fails, then you will need to manually roll back successful checkAndSet operations,
     * as data will have been overwritten.
     * It is therefore not recommended to attempt to perform checkAndSet operations alongside other operations in a
     * single transaction.
     * <p>
     * If the call completes successfully, then you know that the Cell initially had the value you expected,
     * although the Cell could have taken on another value and then been written back to the expected value since
     * said value was obtained.
     * If a {@link CheckAndSetException} is thrown, it is likely that the value stored was not as you expected.
     * In this case, you may want to check the stored value and determine why it was different from the expected value.
     *
     * @param checkAndSetRequest the request, including table, cell, old value and new value.
     * @throws CheckAndSetException if the stored value for the cell was not as expected.
     */
    @POST
    @Path("check-and-set")
    @Consumes(MediaType.APPLICATION_JSON)
    void checkAndSet(CheckAndSetRequest checkAndSetRequest) throws CheckAndSetException;

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
     *  @param tableRef the name of the table to delete values from.
     * @param keys map containing the keys to delete values for; the map should specify, for each
     */
    @POST
    @Path("delete")
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    void delete(@QueryParam("tableRef") TableReference tableRef, Multimap<Cell, Long> keys);

    /**
     * Deletes values in a range from the key-value store.
     *
     * Does not guarantee an atomic delete throughout the entire range.
     *
     * Currently does not allow a column selection to mean only delete certain columns in a range.
     *
     * Some systems may require more nodes to be up to ensure that a delete is successful. If this
     * is the case then this method may throw if the delete can't be completed on all nodes.
     *
     * @param tableRef the name of the table to delete values from.
     * @param range the range to delete
     */
    @POST
    @Path("delete-range")
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    void deleteRange(@QueryParam("tableRef") TableReference tableRef, RangeRequest range);

    /**
     * For each cell, deletes all timestamps prior to the associated maximum timestamp, excluding garbage collection
     * sentinels. Depending on the implementation, this may result in a range tombstone in the underlying KVS.
     */
    @POST
    @Path("delete-all-timestamps")
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    void deleteAllTimestamps(@QueryParam("tableRef") TableReference tableRef,
            Map<Cell, Long> maxTimestampExclusiveByCell);

    /**
     * Truncate a table in the key-value store.
     * <p>
     * This is preferred to dropping and re-adding a table, as live schema changes can
     * be a complicated topic for distributed databases.
     *
     * @param tableRef the name of the table to truncate.
     *
     * @throws InsufficientConsistencyException if not all hosts respond successfully
     * @throws RuntimeException or a subclass of RuntimeException if the table does not exist
     */
    @POST
    @Path("truncate-table")
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    void truncateTable(@QueryParam("tableRef") TableReference tableRef) throws InsufficientConsistencyException;

    /**
     * Truncate tables in the key-value store.
     * <p>
     * This can be slightly faster than repeatedly truncating individual tables.
     *
     * @param tableRefs the name of the tables to truncate.
     *
     * @throws InsufficientConsistencyException if not all hosts respond successfully
     * @throws RuntimeException or a subclass of RuntimeException if the table does not exist
     */
    @POST
    @Path("truncate-tables")
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    void truncateTables(Set<TableReference> tableRefs) throws InsufficientConsistencyException;

    /**
     * For each row in the specified range, returns the most recent version strictly before
     * timestamp.
     *
     * Remember to close any {@link ClosableIterator}s you get in a finally block.
     * @param rangeRequest the range to load.
     * @param timestamp specifies the maximum timestamp (exclusive) at which to retrieve each rows's
     */
    @POST
    @Path("get-range")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    ClosableIterator<RowResult<Value>> getRange(@QueryParam("tableRef") TableReference tableRef,
                                                RangeRequest rangeRequest,
                                                @QueryParam("timestamp") long timestamp);

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
     * @param tableRef the name of the table to read from.
     * @param rangeRequest the range to load.
     * @param timestamp the maximum timestamp to load.
     *
     * @throws InsufficientConsistencyException if not all hosts respond successfully
     *
     * @deprecated use {@link #getCandidateCellsForSweeping}
     */
    @POST
    @Path("get-range-of-timestamps")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    @Deprecated
    ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(
            @QueryParam("tableRef") TableReference tableRef,
            RangeRequest rangeRequest,
            @QueryParam("timestamp") long timestamp) throws InsufficientConsistencyException;

    /**
     * For a given range of rows, returns all candidate cells for sweeping (and their timestamps).
     * <p>
     * A candidate cell is a cell that has at least one timestamp that is less than request.sweepTimestamp() and is
     * not in the set specified by request.timestampsToIgnore().
     * <p>
     * This method will scan the semi-open range of rows from the start row specified in the {@code request}
     * to the end of the table. If the given start row name is an empty byte array, the whole table will be
     * scanned.
     * <p>
     * The returned cells will be lexicographically ordered.
     * <p>
     * We return an iterator of lists instead of a "flat" iterator of results so that we preserve the information
     * about batching. The caller can always use Iterators.concat() or similar if this is undesired.
     */
    ClosableIterator<List<CandidateCellForSweeping>> getCandidateCellsForSweeping(
            TableReference tableRef,
            CandidateCellForSweepingRequest request);

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
    @POST
    @Path("get-first-batch-for-ranges")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            @QueryParam("tableRef") TableReference tableRef,
            Iterable<RangeRequest> rangeRequests,
            @QueryParam("timestamp") long timestamp);

    ////////////////////////////////////////////////////////////
    // TABLE CREATION AND METADATA
    ////////////////////////////////////////////////////////////

    /**
     * Drop the table, and also delete its table metadata.
     *
     * Do not fall into the trap of performing drop & immediate re-create of tables;
     * instead use 'truncate' for this task.
     */
    @DELETE
    @Path("drop-table")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    void dropTable(@QueryParam("tableName") TableReference tableRef) throws InsufficientConsistencyException;


    /**
     * Drops many tables in idempotent fashion. If you are dropping many tables at once,
     * use this call as the implementation can be much faster/less error-prone on some KVSs.
     * Also deletes corresponding table metadata.
     *
     * Do not fall into the trap of performing drop & immediate re-create of tables;
     * instead use 'truncate' for this task.
     */
    @DELETE
    @Path("drop-tables")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    void dropTables(Set<TableReference> tableRefs) throws InsufficientConsistencyException;

    /**
     * Creates a table with the specified name. If the table already exists, no action is performed
     * (the table is left in its current state).
     */
    @POST
    @Path("create-table")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    void createTable(@QueryParam("tableRef") TableReference tableRef, byte[] tableMetadata)
            throws InsufficientConsistencyException;

    /**
     * Creates many tables in idempotent fashion. If you are making many tables at once,
     * use this call as the implementation can be much faster/less error-prone on some KVSs.
     */
    @POST
    @Path("create-tables")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    void createTables(Map<TableReference, byte[]> tableRefToTableMetadata) throws InsufficientConsistencyException;

    /**
     * Return the list of tables stored in this key value service.
     *
     * This will contain system tables (such as the _transaction table), but will not contain
     * the names of any tables used internally by the key value service (a common example is
     * a _metadata table for storing table metadata).
     */
    @POST
    @Path("get-all-table-names")
    @Produces(MediaType.APPLICATION_JSON)
    @Idempotent
    Set<TableReference> getAllTableNames();

    /**
     * Gets the metadata for a given table. Also useful for checking to see if a table exists.
     *
     * @return a byte array representing the metadata for the table. Array is empty if no table
     * with the given name exists. Consider {@link TableMetadata#BYTES_HYDRATOR} for hydrating.
     */
    @Idempotent
    @POST
    @Path("get-metadata-for-table")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    byte[] getMetadataForTable(@QueryParam("tableRef") TableReference tableRef);

    /**
     * Gets the metadata for all known user-created Atlas tables.
     * Consider not using this if you will be running against an Atlas instance with a large number of tables.
     *
     * @return a Map from TableReference to byte array representing the metadata for the table
     * Consider {@link TableMetadata#BYTES_HYDRATOR} for hydrating
     */
    @POST
    @Path("get-metadata-for-tables")
    @Produces(MediaType.APPLICATION_JSON)
    @Idempotent
    Map<TableReference, byte[]> getMetadataForTables();

    @POST
    @Path("put-metadata-for-table")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Idempotent
    void putMetadataForTable(@QueryParam("tableRef") TableReference tableRef, byte[] metadata);

    @POST
    @Path("put-metadata-for-tables")
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    void putMetadataForTables(Map<TableReference, byte[]> tableRefToMetadata);

    ////////////////////////////////////////////////////////////
    // METHODS TO SUPPORT GARBAGE COLLECTION
    ////////////////////////////////////////////////////////////

    /**
     * Adds a value with timestamp = Value.INVALID_VALUE_TIMESTAMP to each of the given cells. If
     * a value already exists at that time stamp, nothing is written for that cell.
     */
    @POST
    @Path("add-gc-sentinel-values")
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    void addGarbageCollectionSentinelValues(@QueryParam("tableName") TableReference tableRef, Iterable<Cell> cells);

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
     * @param tableRef the name of the table to retrieve timestamps from.
     * @param cells set containg cells to retrieve timestamps for.
     * @param timestamp maximum timestamp to get (exclusive)
     * @return multimap of timestamps by cell
     *
     * @throws InsufficientConsistencyException if not all hosts respond successfully
     */
    @POST
    @Path("get-all-timestamps")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    Multimap<Cell, Long> getAllTimestamps(@QueryParam("tableName") TableReference tableRef,
                                          Set<Cell> cells,
                                          @QueryParam("timestamp") long timestamp)
            throws InsufficientConsistencyException;

    /**
     * Does whatever can be done to compact or cleanup a table. Intended to be called after many
     * deletions are performed.
     *
     * This call must be implemented so that it completes synchronously.
     */
    @POST
    @Path("compact-internally")
    @Consumes(MediaType.APPLICATION_JSON)
    void compactInternally(TableReference tableRef);

    /**
     * Some compaction operations might make block reads and writes.
     * These operations will just trigger when inSafeHours is set to true.
     */
    default void compactInternally(TableReference tableRef, boolean inSafeHours) {
        compactInternally(tableRef);
    }

    /**
     * Provides a {@link ClusterAvailabilityStatus}, indicating the current availability of the key value store.
     * This can be used to infer product health - in the usual, conservative case, products can call
     * {@link ClusterAvailabilityStatus#isHealthy()}, which returns true only if all KVS nodes are up.
     * <p>
     * Products that use AtlasDB only for reads and writes (no schema mutations or deletes, including having sweep and
     * scrub disabled) can also treat {@link ClusterAvailabilityStatus#QUORUM_AVAILABLE} as healthy.
     * <p>
     * If you have access to a {@link com.palantir.atlasdb.transaction.api.TransactionManager}, then it is recommended
     * to use its availability indicator, {@link TransactionManager#getKeyValueServiceStatus()}, instead of this one.
     * <p>
     * This call must be implemented so that it completes synchronously.
     */
    @POST
    @Path("node-availability-status")
    @Consumes(MediaType.APPLICATION_JSON)
    ClusterAvailabilityStatus getClusterAvailabilityStatus();

    ////////////////////////////////////////////////////////////
    // SPECIAL CASING SOME KVSs
    ////////////////////////////////////////////////////////////

    /**
     * @return true iff the KeyValueService has been initialized and is ready to use
     *         Note that this check ignores the cluster's availability - use {@link #getClusterAvailabilityStatus()} if
     *         you wish to verify that we can talk to the backing store.
     */
    default boolean isInitialized() {
        return true;
    }

    /**
     * Whether or not read performance degrades significantly when many deleted cells are in the requested range.
     * This is used by sweep to determine if it should wait a while between runs after deleting a large number of cells.
     */
    default boolean performanceIsSensitiveToTombstones() {
        return false;
    }

    /**
     * @return If {@link #compactInternally(TableReference)} should be called to free disk space.
     */
    default boolean shouldManuallyCompact() {
        return true;
    }
}
