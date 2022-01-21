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

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.lock.watch.CommitUpdate;
import java.util.Map;
import java.util.Set;

/**
 * Encapsulates the local cached reads held within a transaction cache. These values can either originate from the
 * central cache (populated by previous transactions), or from the current transaction's reads and writes. Note that
 * this store also caches the absence of a value as an empty {@link CacheValue}.
 */
interface TransactionCacheValueStore {
    boolean isWatched(TableReference tableReference);

    void cacheRemoteWrite(TableReference tableReference, Cell cell);

    void cacheRemoteReads(TableReference tableReference, Map<Cell, byte[]> remoteReadValues);

    void cacheEmptyReads(TableReference tableReference, Set<Cell> emptyCells);

    Map<Cell, CacheValue> getCachedValues(TableReference table, Set<Cell> cells);

    /**
     * Contains a map of all the values that were read remotely and stored locally (filtering out those that were
     * unable to be cached due to values being locked). Also note that writes do not appear in the digest.
     */
    Map<CellReference, CacheValue> getValueDigest();

    Set<CellReference> getHitDigest();

    TransactionCacheValueStore createWithFilteredSnapshot(CommitUpdate commitUpdate);
}
