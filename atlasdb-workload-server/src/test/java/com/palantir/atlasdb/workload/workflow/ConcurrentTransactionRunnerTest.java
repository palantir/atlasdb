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

package com.palantir.atlasdb.workload.workflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.workload.store.AtlasDbTransactionStore;
import com.palantir.atlasdb.workload.transaction.witnessed.ImmutableWitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Before;
import org.junit.Test;

public class ConcurrentTransactionRunnerTest {
    private static final int DEFAULT_TASK_MULTIPLICITY = 100;

    private final IndexedTransactionTask task = mock(IndexedTransactionTask.class);
    private final AtlasDbTransactionStore store = mock(AtlasDbTransactionStore.class);
    private final DeterministicScheduler scheduler = new DeterministicScheduler();
    private final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(scheduler);
    private final ConcurrentTransactionRunner runner = new ConcurrentTransactionRunner(store, executorService);

    @Before
    public void setupTask() {
        when(task.apply(any(), anyInt()))
                .thenAnswer(invocation ->
                        Optional.of(createWitnessedTransactionWithStartTimestamp(invocation.getArgument(1))));
    }

    @Test
    public void throwsIfRunningTaskNegativeNumberOfTimes() {
        assertThatThrownBy(() -> runner.runConcurrentTransactionTask(task, -1))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining("Tasks must be run a non-negative number of times");
        verifyNoInteractions(task);
    }

    @Test
    public void runsTaskAndAggregatesResults() {
        Future<List<WitnessedTransaction>> witnessedTransactionsFuture =
                runner.runConcurrentTransactionTask(task, DEFAULT_TASK_MULTIPLICITY);
        assertThat(witnessedTransactionsFuture).isNotDone();
        scheduler.runUntilIdle();
        assertThat(witnessedTransactionsFuture).isDone();
        List<WitnessedTransaction> witnessedTransactions = Futures.getUnchecked(witnessedTransactionsFuture);
        assertThat(witnessedTransactions)
                .containsExactlyElementsOf(IntStream.range(0, DEFAULT_TASK_MULTIPLICITY)
                        .mapToObj(ConcurrentTransactionRunnerTest::createWitnessedTransactionWithStartTimestamp)
                        .collect(Collectors.toList()));
    }

    @Test
    public void resultFiltersOutTransactionsThatCouldNotBeWitnessed() {
        Set<Integer> failingTasks = ImmutableSet.of(3, 7, 11, 27, 41);
        failingTasks.forEach(
                failingTask -> when(task.apply(any(), eq(failingTask))).thenReturn(Optional.empty()));

        Future<List<WitnessedTransaction>> witnessedTransactionsFuture =
                runner.runConcurrentTransactionTask(task, DEFAULT_TASK_MULTIPLICITY);
        scheduler.runUntilIdle();
        List<WitnessedTransaction> witnessedTransactions = Futures.getUnchecked(witnessedTransactionsFuture);
        assertThat(witnessedTransactions)
                .containsExactlyElementsOf(IntStream.range(0, DEFAULT_TASK_MULTIPLICITY)
                        .filter(index -> !failingTasks.contains(index))
                        .mapToObj(ConcurrentTransactionRunnerTest::createWitnessedTransactionWithStartTimestamp)
                        .collect(Collectors.toList()));
    }

    @Test
    public void aggregatedResultWaitsForIndividuallySlowTransactions() {
        Future<List<WitnessedTransaction>> witnessedTransactionsFuture =
                runner.runConcurrentTransactionTask(task, DEFAULT_TASK_MULTIPLICITY);
        for (int iteration = 0; iteration < DEFAULT_TASK_MULTIPLICITY; iteration++) {
            assertThat(witnessedTransactionsFuture).isNotDone();
            scheduler.runNextPendingCommand();
        }
        assertThat(witnessedTransactionsFuture).isDone();
    }

    @Test
    public void storePassedThroughToIndividualTasks() {
        when(task.apply(any(), anyInt())).thenAnswer(invocation -> {
            AtlasDbTransactionStore transactionStore = invocation.getArgument(0);
            transactionStore.readWrite(ImmutableList.of());
            return Optional.of(createWitnessedTransactionWithStartTimestamp(invocation.getArgument(1)));
        });
        runner.runConcurrentTransactionTask(task, DEFAULT_TASK_MULTIPLICITY);
        scheduler.runUntilIdle();
        verify(store, times(DEFAULT_TASK_MULTIPLICITY)).readWrite(ImmutableList.of());
    }

    private static WitnessedTransaction createWitnessedTransactionWithStartTimestamp(int startTimestamp) {
        return ImmutableWitnessedTransaction.builder()
                .startTimestamp(startTimestamp)
                .build();
    }
}
