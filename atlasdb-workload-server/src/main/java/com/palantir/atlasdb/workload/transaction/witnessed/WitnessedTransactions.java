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

package com.palantir.atlasdb.workload.transaction.witnessed;

import com.palantir.atlasdb.workload.store.ReadOnlyTransactionStore;
import java.util.Comparator;
import java.util.List;
import one.util.streamex.StreamEx;

public final class WitnessedTransactions {
    private WitnessedTransactions() {
        // utility
    }

    /**
     * Sorts transactions by their effective timestamp and filters out transactions that are not fully committed.
     */
    public static List<FullyWitnessedTransaction> sortAndFilterTransactions(
            ReadOnlyTransactionStore readOnlyTransactionStore, List<WitnessedTransaction> unorderedTransactions) {
        OnlyCommittedWitnessedTransactionVisitor visitor =
                new OnlyCommittedWitnessedTransactionVisitor(readOnlyTransactionStore);
        return StreamEx.of(unorderedTransactions)
                .mapPartial(transaction -> transaction.accept(visitor))
                .sorted(Comparator.comparingLong(WitnessedTransactions::effectiveTimestamp))
                .toList();
    }

    private static long effectiveTimestamp(WitnessedTransaction witnessedTransaction) {
        return witnessedTransaction.commitTimestamp().orElseGet(witnessedTransaction::startTimestamp);
    }
}
