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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.workload.store.ReadOnlyTransactionStore;
import java.util.List;
import org.junit.Test;
import org.mockito.Mock;

public class WitnessedTransactionsTest {
    @Mock
    private ReadOnlyTransactionStore readOnlyTransactionStore;

    @Test
    public void returnsUnderlyingWitnessedTransactions() {
        FullyWitnessedTransaction twoToEight = createWitnessedTransactionWithoutActions(2, 8);
        FullyWitnessedTransaction readOnlyAtFive = createReadOnlyWitnessedTransactionWithoutActions(5);

        assertThat(WitnessedTransactions.sortAndFilterTransactions(
                        readOnlyTransactionStore, ImmutableList.of(twoToEight)))
                .containsExactly(twoToEight);
        assertThat(WitnessedTransactions.sortAndFilterTransactions(
                        readOnlyTransactionStore, ImmutableList.of(readOnlyAtFive)))
                .containsExactly(readOnlyAtFive);
    }

    @Test
    public void sortsResultsOfWriteTransactionsByCommitTimestamp() {
        FullyWitnessedTransaction twoToEight = createWitnessedTransactionWithoutActions(2, 8);
        FullyWitnessedTransaction threeToSeven = createWitnessedTransactionWithoutActions(3, 7);
        FullyWitnessedTransaction fourToSix = createWitnessedTransactionWithoutActions(4, 6);

        assertThat(WitnessedTransactions.sortAndFilterTransactions(
                        readOnlyTransactionStore, ImmutableList.of(twoToEight, threeToSeven, fourToSix)))
                .containsExactly(fourToSix, threeToSeven, twoToEight);
    }

    @Test
    public void sortsReadOnlyTransactionsByStartTimestamp() {
        FullyWitnessedTransaction readOnlyAtThree = createReadOnlyWitnessedTransactionWithoutActions(3);
        FullyWitnessedTransaction readOnlyAtSeven = createReadOnlyWitnessedTransactionWithoutActions(7);
        FullyWitnessedTransaction readOnlyAtFortyTwo = createReadOnlyWitnessedTransactionWithoutActions(42);

        assertThat(WitnessedTransactions.sortAndFilterTransactions(
                        readOnlyTransactionStore,
                        ImmutableList.of(readOnlyAtSeven, readOnlyAtFortyTwo, readOnlyAtThree)))
                .containsExactly(readOnlyAtThree, readOnlyAtSeven, readOnlyAtFortyTwo);
    }

    @Test
    public void sortsReadAndWriteTransactionsByEffectiveTimestamp() {
        FullyWitnessedTransaction twoToEight = createWitnessedTransactionWithoutActions(2, 8);
        FullyWitnessedTransaction fourToSix = createWitnessedTransactionWithoutActions(4, 6);

        FullyWitnessedTransaction readOnlyAtThree = createReadOnlyWitnessedTransactionWithoutActions(3);
        FullyWitnessedTransaction readOnlyAtFive = createReadOnlyWitnessedTransactionWithoutActions(5);
        FullyWitnessedTransaction readOnlyAtSeven = createReadOnlyWitnessedTransactionWithoutActions(7);
        FullyWitnessedTransaction readOnlyAtNine = createReadOnlyWitnessedTransactionWithoutActions(9);

        assertThat(WitnessedTransactions.sortAndFilterTransactions(
                        readOnlyTransactionStore,
                        ImmutableList.of(
                                twoToEight,
                                readOnlyAtNine,
                                fourToSix,
                                readOnlyAtFive,
                                readOnlyAtThree,
                                readOnlyAtSeven)))
                .containsExactly(
                        readOnlyAtThree, readOnlyAtFive, fourToSix, readOnlyAtSeven, twoToEight, readOnlyAtNine);
    }

    @Test
    public void filterRemovesUncommittedTransactions() {
        MaybeWitnessedTransaction committedMaybeWitnessedTransaction =
                createMaybeWitnessedTransactionWithoutActions(1, 3, true);
        MaybeWitnessedTransaction notCommittedTransaction = createMaybeWitnessedTransactionWithoutActions(2, 8, false);
        FullyWitnessedTransaction readOnlyAtFive = createReadOnlyWitnessedTransactionWithoutActions(5);
        FullyWitnessedTransaction fourToSix = createWitnessedTransactionWithoutActions(4, 6);
        assertThat(WitnessedTransactions.sortAndFilterTransactions(
                        readOnlyTransactionStore,
                        List.of(
                                committedMaybeWitnessedTransaction,
                                notCommittedTransaction,
                                readOnlyAtFive,
                                fourToSix)))
                .containsExactly(committedMaybeWitnessedTransaction.toFullyWitnessed(), readOnlyAtFive, fourToSix);
    }

    private MaybeWitnessedTransaction createMaybeWitnessedTransactionWithoutActions(
            long start, long commit, boolean committed) {
        when(readOnlyTransactionStore.isCommitted(eq(start))).thenReturn(committed);
        return MaybeWitnessedTransaction.builder()
                .startTimestamp(start)
                .commitTimestamp(commit)
                .build();
    }

    private static FullyWitnessedTransaction createReadOnlyWitnessedTransactionWithoutActions(long start) {
        return FullyWitnessedTransaction.builder().startTimestamp(start).build();
    }

    private static FullyWitnessedTransaction createWitnessedTransactionWithoutActions(long start, long commit) {
        return FullyWitnessedTransaction.builder()
                .startTimestamp(start)
                .commitTimestamp(commit)
                .build();
    }
}
