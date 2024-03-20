/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.api;

import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import javax.annotation.Nullable;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.map.primitive.LongLongMap;

public interface CommitTimestampLoader {
    /**
     * Returns a map from start timestamp to commit timestamp. If the transaction corresponding to a start timestamp
     * has neither committed nor aborted, it will be missing from the map. This method will block until
     * the transactions for these start timestamps are believed to no longer be running.
     *
     * Note that this method does not actively abort transactions - in particular, a transaction that is believed to
     * no longer be running may still commit in the future.
     */
    ListenableFuture<LongLongMap> getCommitTimestamps(@Nullable TableReference tableRef, LongIterable startTimestamps);

    /**
     * Performs {@link #getCommitTimestamps(TableReference, LongIterable)} as described above, but does not wait for
     * transactions for these start timestamps to be believed to no longer be running. This is likely to be relevant
     * in contexts such as conflict checking, where we want to check for conflicts with transactions that are still
     * running while avoiding possible deadlocks between transactions reading or writing to overlapping sets of cells.
     */
    ListenableFuture<LongLongMap> getCommitTimestampsNonBlockingForValidation(
            @Nullable TableReference tableRef, LongIterable startTimestamps);
}
