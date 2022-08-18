/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

/**
 * General condition that is checked before a transaction commits for validity. All conditions associated with a
 * transaction must be true for that transaction to commit successfully. If a given condition is valid at commit
 * time, it must also be true that it was valid throughout the lifetime of the transaction. This is not explicitly
 * checked, so care must be taken in designing appropriate conditions that satisfy this constraint.
 *
 * As an example, advisory locks are a common pre-commit condition and one that is built into the
 * {@link TransactionManager} API. These locks are granted before the transaction starts and cannot be refreshed
 * once they lapse, so they provide the necessary invariant. If they are valid at commit time, they were valid
 * for the entire lifetime of the transaction.
 *
 * Conditions are associated with a transaction, but should not interfere with the transaction locks (non-advisory)
 * used by the transaction. The validity of those locks is controlled separately from any pre-commit conditions, and
 * they follow different constraints - transaction locks are only acquired at commit time, for example, rather than
 * at the beginning of a transaction.
 */
@FunctionalInterface
public interface PreCommitCondition {

    /**
     * Checks that the condition is valid at the given timestamp, otherwise throws a
     * {@link TransactionFailedException}. If the condition is not valid, the transaction will not be committed.
     */
    void throwIfConditionInvalid(long timestamp);

    /**
     * Cleans up any state managed by this condition, e.g. a lock that should be held for the lifetime of the
     * transaction. Conditions last the lifetime of a particular transaction and will be cleaned up on each retry.
     *
     * The cleanup method should not throw any exceptions - it should be best effort. When this method is called,
     * the transaction has already failed or committed.
     *
     * If the cleanup is run, then the cleanup is guaranteed to run before any
     * {@link Transaction#onSuccess(Runnable)} callbacks, <i>if {@link Transaction#commit()} was not called during a
     * transaction task.
     * </i>
     */
    default void cleanup() {}
}
