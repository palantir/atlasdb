/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.api.purity;

import org.immutables.value.Value;

/**
 * Be VERY careful about labelling transactions pure, in particular being "resilient to momentarily inconsistent reads
 * on doomed transactions". This is a pretty strong guarantee: if you do significant processing in a transaction, you
 * might get garbage results, and even things you think are pure might not be. For example, an invariant that
 * exactly one of two cells must be present could be invalidated if you read the first (empty) cell, and a concurrent
 * transaction writes the first cell and deletes the second cell before you read the second cell.
 */
@Value.Immutable(intern = true)
public interface TransactionPurity {
    static TransactionPurity pure() {
        return ImmutableTransactionPurity.builder()
                .doesNotHaveSideEffects(true)
                .resilientToMomentarilyInconsistentReadsOnDoomedTransactions(true)
                .build();
    }

    static TransactionPurity impure() {
        return ImmutableTransactionPurity.builder()
                .doesNotHaveSideEffects(false)
                .resilientToMomentarilyInconsistentReadsOnDoomedTransactions(false)
                .build();
    }

    boolean doesNotHaveSideEffects();

    boolean resilientToMomentarilyInconsistentReadsOnDoomedTransactions();

    @Value.Lazy
    default boolean canSkipIntermediateImmutableLockChecksOnThoroughTables() {
        return doesNotHaveSideEffects() && resilientToMomentarilyInconsistentReadsOnDoomedTransactions();
    }
}
