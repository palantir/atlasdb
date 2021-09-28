/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.keyvalue.api.cache;

import com.google.common.primitives.UnsignedBytes;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.lock.watch.CommitUpdate;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.Function;

/**
 * The {@link LockWatchValueScopingCache} will provide one of these to every (relevant) transaction, and this will
 * contain a view of the cache at the correct point in time, which is determined by the transaction's
 * {@link com.palantir.lock.watch.LockWatchVersion}.
 *
 * The semantics of this cache are as follows:
 *  1. Writes in the transaction will invalidate values in this local cache (but *not* in the central cache, as the
 *     lock events will take care of that);
 *  2. Reads will first try to read a value from the cache; if not present, it will be loaded remotely and cached
 *     locally.
 *  3. At commit time, all remote reads will be flushed to the central cache (and these reads will be filtered based on
 *     the lock events that have happened since the transaction was started).
 *
 * It is safe to perform these reads due to the transaction semantics: for a serialisable transaction, we will use
 * the locked descriptors at commit time to validate whether we have read-write conflicts; otherwise, reading the
 * values in the cache is just as safe as reading a snapshot of the database taken at the start timestamp.
 */
public interface TransactionScopedCache {
    void write(TableReference tableReference, Map<Cell, byte[]> values);

    void delete(TableReference tableReference, Set<Cell> cells);

    /**
     * This should be used for performing *synchronous* gets. The reason the value loader function returns a listenable
     * future is to optimise parallel requests to the cache: only the code that directly affects the state of the
     * cache is synchronised, while loads from the database are done outside synchronised blocks.
     */
    Map<Cell, byte[]> get(
            TableReference tableReference,
            Set<Cell> cells,
            Function<Set<Cell>, ListenableFuture<Map<Cell, byte[]>>> valueLoader);

    ListenableFuture<Map<Cell, byte[]>> getAsync(
            TableReference tableReference,
            Set<Cell> cells,
            Function<Set<Cell>, ListenableFuture<Map<Cell, byte[]>>> valueLoader);

    /**
     * The cache will try to fulfil as much of the request as possible with cached values. In the case where some of the
     * columns are present in the cache for a row, the cellLoader will be used to read the remaining cells remotely.
     * For rows that have none of the columns present in the cache, the rowLoader will be used. Note that this may
     * result in two KVS reads, but it is expected to be offset by not having to read the already cached values.
     *
     * The result map uses {@link UnsignedBytes#lexicographicalComparator()} on the keys, so there will be no
     * duplicate rows, even if duplicates were specified in rows. Any row with no columns present will be absent in
     * the result map, as long as the rowLoader behaves in the same way.
     */
    NavigableMap<byte[], RowResult<byte[]>> getRows(
            TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnSelection columnSelection,
            Function<Set<Cell>, Map<Cell, byte[]>> cellLoader,
            Function<Iterable<byte[]>, NavigableMap<byte[], RowResult<byte[]>>> rowLoader);

    /**
     * This method should be called before retrieving the value or hit digest, as it guarantees that no more reads or
     * writes will be performed on the cache. This method is idempotent, and may legitimately be called multiple times.
     */
    void finalise();

    ValueDigest getValueDigest();

    HitDigest getHitDigest();

    TransactionScopedCache createReadOnlyCache(CommitUpdate commitUpdate);
}
