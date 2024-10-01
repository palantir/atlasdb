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

import java.util.function.Consumer;

public interface TimestampLockAwareTransaction {
    /**
     * Similar to {@link Transaction#preCommit(Runnable)} with the option of fetching timestamps.
     * <p>
     * A lock will be taken on a timestamp before all timestamps provided in {@code preCommitAction}.
     * This lock will be checked at commit time, and if expired, will fail the transaction.
     * <p>
     * Clients can use {@link #getLockedTimestamp(String)} to fetch the earliest locked
     * timestamp for a given {@param timestampLockDescriptor} of an open transaction.
     * Note these semantics are the quite similar to {@link TransactionManager#getImmutableTimestamp()}, but instead
     * of tracking open start transaction timestamps, we track open {@code preCommitAction} timestamps.
     *
     * @param timestampLockDescriptor the string representing the timestampLockDescriptor workflow
     * @param timestampCount the number of timestamps that will be fetched in the pre-commit hook.
     * @param preCommitAction the lambda executed just before commit
     *
     * @throws RuntimeException If requesting more timestamps in {@code preCommitAction} than specified in
     * timestampCount.
     */
    void preCommit(String timestampLockDescriptor, int timestampCount, Consumer<TimestampSupplier> preCommitAction);

    interface TimestampSupplier {
        long getTimestamp();
    }

    /**
     * See {@link #preCommit(String, int, Consumer)} for more details.
     * If no transactions with a {@code timestampLockDescriptor} lock are open, then we'd return a new fresh timestamp
     * - equivalent to {@link Transaction#getTimestamp()}.
     *
     * @param timestampLockDescriptor the string representing the timestampLockDescriptor workflow
     * @return the latest timestamp for which there are no open preCommitAction timestamps.
     */
    long getLockedTimestamp(String timestampLockDescriptor);
}
