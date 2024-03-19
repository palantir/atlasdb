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

package com.palantir.atlasdb.transaction.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.CommitTimestampLoader;
import com.palantir.atlasdb.transaction.impl.metrics.TransactionMetrics;
import com.palantir.atlasdb.transaction.impl.metrics.TransactionOutcomeMetrics;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.factory.primitive.LongLists;
import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.map.primitive.LongLongMap;
import org.eclipse.collections.api.map.primitive.MutableLongLongMap;
import org.eclipse.collections.impl.factory.primitive.LongLongMaps;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.immutables.value.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public final class ReadValidationCommitTimestampLoaderTest {
    private static final long START_TS = 100L;
    private static final long COMMIT_TS = 200L;

    private static final long BEFORE_START_1 = 50L;
    private static final long BEFORE_START_2 = 75L;
    private static final long BETWEEN_START_AND_COMMIT_1 = 125L;
    private static final long BETWEEN_START_AND_COMMIT_2 = 175L;
    private static final long AFTER_COMMIT = 250L;
    private static final long TRANSACTION_INTERVAL = 42L;

    private final MutableLongLongMap committedTransactions = LongLongMaps.mutable.empty();

    private final MetricsManager metricsManager = MetricsManagers.createForTests();

    private CommitTimestampLoader delegateCommitTimestampLoader;

    private ReadValidationCommitTimestampLoader commitTimestampLoader;

    @BeforeEach
    public void setUp() {
        delegateCommitTimestampLoader = spy(new MemoryCommitTimestampLoader());
        commitTimestampLoader = new ReadValidationCommitTimestampLoader(
                delegateCommitTimestampLoader,
                START_TS,
                COMMIT_TS,
                TransactionOutcomeMetrics.create(
                        TransactionMetrics.of(metricsManager.getTaggedRegistry()), metricsManager.getTaggedRegistry()));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void returnsNothingIfQueriedForNothing(boolean shouldWaitForCommitterToComplete)
            throws ExecutionException, InterruptedException {
        CommitTimestampLoadingMethod commitTimestampLoadingMethod = getLoadingMethod(shouldWaitForCommitterToComplete);
        LongLongMap result = commitTimestampLoadingMethod
                .commitTimestampLoadingMethod()
                .apply(LongLists.immutable.empty())
                .get();

        assertThat(result).isEqualTo(LongLongMaps.immutable.empty());
        verifyNoInteractions(delegateCommitTimestampLoader);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void returnsOurCommitTimestampIfQueriedForOurStart(boolean shouldWaitForCommitterToComplete)
            throws ExecutionException, InterruptedException {
        CommitTimestampLoadingMethod commitTimestampLoadingMethod = getLoadingMethod(shouldWaitForCommitterToComplete);

        LongLongMap result = commitTimestampLoadingMethod
                .commitTimestampLoadingMethod()
                .apply(LongLists.immutable.of(START_TS))
                .get();

        assertThat(result).isEqualTo(LongLongMaps.immutable.of(START_TS, COMMIT_TS));
        verifyNoInteractions(delegateCommitTimestampLoader);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void waitsForTransactionsStartingBeforeOurStart(boolean shouldWaitForCommitterToComplete)
            throws ExecutionException, InterruptedException {
        CommitTimestampLoadingMethod loadingMethod = getLoadingMethod(shouldWaitForCommitterToComplete);
        committedTransactions.put(BEFORE_START_1, AFTER_COMMIT);
        LongLongMap result = loadingMethod
                .commitTimestampLoadingMethod()
                .apply(LongSets.immutable.of(BEFORE_START_1))
                .get();

        assertThat(result).isEqualTo(LongLongMaps.immutable.of(BEFORE_START_1, AFTER_COMMIT));
        loadingMethod.loaderVerification().accept(LongSets.immutable.of(BEFORE_START_1));
        verifyNoMoreInteractions(delegateCommitTimestampLoader);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void doesNotWaitForTransactionsStartingAfterOurStart(boolean shouldWaitForCommitterToComplete)
            throws ExecutionException, InterruptedException {
        CommitTimestampLoadingMethod loadingMethod = getLoadingMethod(shouldWaitForCommitterToComplete);

        committedTransactions.put(BETWEEN_START_AND_COMMIT_1, BETWEEN_START_AND_COMMIT_2);
        LongLongMap result = loadingMethod
                .commitTimestampLoadingMethod()
                .apply(LongSets.immutable.of(BETWEEN_START_AND_COMMIT_1))
                .get();

        assertThat(result).isEqualTo(LongLongMaps.immutable.of(BETWEEN_START_AND_COMMIT_1, BETWEEN_START_AND_COMMIT_2));

        verify(delegateCommitTimestampLoader)
                .getCommitTimestampsNonBlockingForValidation(null, LongSets.immutable.of(BETWEEN_START_AND_COMMIT_1));
        verifyNoMoreInteractions(delegateCommitTimestampLoader);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void delegatesCallsToAppropriateRangesAndCombinesResults(boolean shouldWaitForCommitterToComplete)
            throws ExecutionException, InterruptedException {
        LongList startTimestamps = LongLists.immutable.of(
                BEFORE_START_1,
                BEFORE_START_2,
                START_TS - 1,
                START_TS,
                START_TS + 1,
                BETWEEN_START_AND_COMMIT_1,
                BETWEEN_START_AND_COMMIT_2,
                AFTER_COMMIT);
        startTimestamps.forEach(startTimestamp -> {
            committedTransactions.put(startTimestamp, startTimestamp + TRANSACTION_INTERVAL);
        });

        CommitTimestampLoadingMethod loadingMethod = getLoadingMethod(shouldWaitForCommitterToComplete);

        LongLongMap result = loadingMethod
                .commitTimestampLoadingMethod()
                .apply(startTimestamps)
                .get();

        assertThat(result).satisfies(startToCommitMap -> {
            assertThat(startToCommitMap.keySet()).isEqualTo(startTimestamps.toSet());
            startToCommitMap.forEachKeyValue((start, commit) -> {
                long expectedCommit = start == START_TS ? COMMIT_TS : start + TRANSACTION_INTERVAL;
                assertThat(commit).isEqualTo(expectedCommit);
            });
        });

        loadingMethod.loaderVerification().accept(LongSets.immutable.of(BEFORE_START_1, BEFORE_START_2, START_TS - 1));

        // Not using the loader verification as reads starting after our start are always intended to be non-blocking
        // to avoid deadlocks.
        verify(delegateCommitTimestampLoader)
                .getCommitTimestampsNonBlockingForValidation(
                        null,
                        LongSets.immutable.of(
                                START_TS + 1, BETWEEN_START_AND_COMMIT_1, BETWEEN_START_AND_COMMIT_2, AFTER_COMMIT));
        verifyNoMoreInteractions(delegateCommitTimestampLoader);
    }

    private CommitTimestampLoadingMethod getLoadingMethod(boolean shouldWaitForCommitterToComplete) {
        return CommitTimestampLoadingMethod.create(
                commitTimestampLoader, delegateCommitTimestampLoader, shouldWaitForCommitterToComplete);
    }

    private final class MemoryCommitTimestampLoader implements CommitTimestampLoader {
        @Override
        public ListenableFuture<LongLongMap> getCommitTimestamps(
                @Nullable TableReference tableRef, LongIterable startTimestamps) {
            return getCommitTimestamps(startTimestamps);
        }

        @Override
        public ListenableFuture<LongLongMap> getCommitTimestampsNonBlockingForValidation(
                @Nullable TableReference tableRef, LongIterable startTimestamps) {
            return getCommitTimestamps(startTimestamps);
        }

        private ListenableFuture<LongLongMap> getCommitTimestamps(LongIterable startTimestamps) {
            MutableLongLongMap result = LongLongMaps.mutable.empty();
            startTimestamps.forEach(startTimestamp -> {
                if (committedTransactions.containsKey(startTimestamp)) {
                    result.put(startTimestamp, committedTransactions.get(startTimestamp));
                }
            });
            return Futures.immediateFuture(result);
        }
    }

    @Value.Immutable
    interface CommitTimestampLoadingMethod {
        Function<LongIterable, ListenableFuture<LongLongMap>> commitTimestampLoadingMethod();

        Consumer<LongIterable> loaderVerification();

        static CommitTimestampLoadingMethod create(
                ReadValidationCommitTimestampLoader readValidationLoader,
                CommitTimestampLoader underlying,
                boolean shouldWaitForCommitterToComplete) {
            return shouldWaitForCommitterToComplete
                    ? blocking(readValidationLoader, underlying)
                    : nonBlocking(readValidationLoader, underlying);
        }

        static CommitTimestampLoadingMethod blocking(
                ReadValidationCommitTimestampLoader readValidationLoader, CommitTimestampLoader underlying) {
            return ImmutableCommitTimestampLoadingMethod.builder()
                    .commitTimestampLoadingMethod(
                            startTimestamps -> readValidationLoader.getCommitTimestamps(null, startTimestamps))
                    .loaderVerification(
                            startTimestamps -> verify(underlying).getCommitTimestamps(null, startTimestamps))
                    .build();
        }

        static CommitTimestampLoadingMethod nonBlocking(
                ReadValidationCommitTimestampLoader readValidationLoader, CommitTimestampLoader underlying) {
            return ImmutableCommitTimestampLoadingMethod.builder()
                    .commitTimestampLoadingMethod(startTimestamps ->
                            readValidationLoader.getCommitTimestampsNonBlockingForValidation(null, startTimestamps))
                    .loaderVerification(startTimestamps ->
                            verify(underlying).getCommitTimestampsNonBlockingForValidation(null, startTimestamps))
                    .build();
        }
    }
}
