/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

@FunctionalInterface
public interface PreCommitConditionWithWatches {
    /**
     * Checks that the condition is valid at the given timestamp, and the changes in lock watch state.
     *
     * Otherwise, throws a {@link TransactionFailedException} and the transaction will not be committed.
     */
    void throwIfConditionInvalid(TimestampWithLockInfo commitTimestampWithLockWatchInfo);

    /**
     * Cleans up any state managed by this condition, e.g. a lock that should be held for the lifetime of the
     * transaction. Conditions last the lifetime of a particular transaction and will be cleaned up on each retry.
     *
     * The cleanup method should not throw any exceptions - it should be best effort. When this method is called,
     * the transaction has already failed or committed.
     */
    default void cleanup() {}

    static PreCommitConditionWithWatches fromPrecommitCondition(PreCommitCondition preCommitCondition) {
        return new PreCommitConditionWithWatches() {
            @Override
            public void throwIfConditionInvalid(TimestampWithLockInfo commitTimestampWithLockWatchInfo) {
                preCommitCondition.throwIfConditionInvalid(commitTimestampWithLockWatchInfo.timestamp());
            }

            @Override
            public void cleanup() {
                preCommitCondition.cleanup();
            }
        };
    }
}
