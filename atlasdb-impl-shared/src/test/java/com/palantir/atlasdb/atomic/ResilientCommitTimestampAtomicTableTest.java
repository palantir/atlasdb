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

package com.palantir.atlasdb.atomic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RetryLimitReachedException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.transaction.encoding.BaseProgressEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.TicketsEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.TwoPhaseEncodingStrategy;
import com.palantir.atlasdb.transaction.service.TransactionStatus;
import com.palantir.common.time.Clock;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ResilientCommitTimestampAtomicTableTest {
    private static final String PARAMETERIZED_TEST_NAME = "validating staging values: {0}";

    private final KeyValueService spiedKvs = spy(new InMemoryKeyValueService(true));
    private final UnreliablePueConsensusForgettingStore spiedStore = spy(new UnreliablePueConsensusForgettingStore(
            spiedKvs, TableReference.createFromFullyQualifiedName("test.table")));

    private AtomicTable<Long, TransactionStatus> atomicTable;
    private final AtomicLong clockLong = new AtomicLong(1000);
    private final Clock clock = clockLong::get;
    private final TwoPhaseEncodingStrategy encodingStrategy =
            new TwoPhaseEncodingStrategy(BaseProgressEncodingStrategy.INSTANCE);

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @ValueSource(booleans = {true, false})
    public void canPutAndGet(boolean validating) throws ExecutionException, InterruptedException {
        setup(validating);
        atomicTable.update(1L, TransactionStatus.committed(2L));
        assertThat(atomicTable.get(1L).get()).isEqualTo(TransactionStatus.committed(2L));
        verify(spiedStore).batchAtomicUpdate(anyMap());
        verify(spiedStore, atLeastOnce()).put(anyMap());
        verify(spiedStore).getMultiple(any());
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @ValueSource(booleans = {true, false})
    public void emptyReturnsInProgress(boolean validating) throws ExecutionException, InterruptedException {
        setup(validating);
        assertThat(atomicTable.get(3L).get()).isEqualTo(TransactionStatus.inProgress());
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @ValueSource(booleans = {true, false})
    public void canPutAndGetAbortedTransactions(boolean validating) throws ExecutionException, InterruptedException {
        setup(validating);
        atomicTable.update(1L, TransactionStatus.aborted());
        assertThat(atomicTable.get(1L).get()).isEqualTo(TransactionStatus.aborted());
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @ValueSource(booleans = {true, false})
    public void cannotPueTwice(boolean validating) {
        setup(validating);
        atomicTable.update(1L, TransactionStatus.committed(2L));
        assertThatThrownBy(() -> atomicTable.update(1L, TransactionStatus.committed(2L)))
                .isInstanceOf(KeyAlreadyExistsException.class);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @ValueSource(booleans = {true, false})
    public void canPutAndGetMultiple(boolean validating) throws ExecutionException, InterruptedException {
        setup(validating);
        ImmutableMap<Long, TransactionStatus> inputs = ImmutableMap.of(
                1L,
                TransactionStatus.committed(2L),
                3L,
                TransactionStatus.committed(4L),
                7L,
                TransactionStatus.committed(8L));
        atomicTable.updateMultiple(inputs);
        Map<Long, TransactionStatus> result =
                atomicTable.get(ImmutableList.of(1L, 3L, 5L, 7L)).get();
        assertThat(result).hasSize(4);
        assertThat(result.get(1L)).isEqualTo(TransactionStatus.committed(2L));
        assertThat(result.get(3L)).isEqualTo(TransactionStatus.committed(4L));
        assertThat(result.get(5L)).isEqualTo(TransactionStatus.inProgress());
        assertThat(result.get(7L)).isEqualTo(TransactionStatus.committed(8L));
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @ValueSource(booleans = {true, false})
    public void pueThatThrowsIsCorrectedOnGet(boolean validating) throws ExecutionException, InterruptedException {
        setup(validating);
        spiedStore.startFailingPuts();
        assertThatThrownBy(() -> atomicTable.update(1L, TransactionStatus.committed(2L)))
                .isInstanceOf(RuntimeException.class);
        spiedStore.stopFailingPuts();

        assertThat(atomicTable.get(1L).get()).isEqualTo(TransactionStatus.committed(2L));
        verify(spiedStore, times(2)).put(anyMap());
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @ValueSource(booleans = {true, false})
    public void getReturnsStagingValuesThatWereCommittedBySomeoneElse(boolean validating)
            throws ExecutionException, InterruptedException {
        setup(validating);
        long startTimestamp = 1L;
        TransactionStatus commitStatus = TransactionStatus.committed(2L);
        Cell timestampAsCell = encodingStrategy.encodeStartTimestampAsCell(startTimestamp);
        byte[] stagingValue =
                encodingStrategy.encodeCommitStatusAsValue(startTimestamp, AtomicValue.staging(commitStatus));
        byte[] committedValue =
                encodingStrategy.encodeCommitStatusAsValue(startTimestamp, AtomicValue.committed(commitStatus));
        spiedStore.atomicUpdate(timestampAsCell, stagingValue);

        List<byte[]> actualValues = ImmutableList.of(committedValue);

        doThrow(new CheckAndSetException("done elsewhere", timestampAsCell, stagingValue, actualValues))
                .when(spiedStore)
                .checkAndTouch(timestampAsCell, stagingValue);

        assertThat(atomicTable.get(startTimestamp).get()).isEqualTo(commitStatus);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @ValueSource(booleans = {true, false})
    public void onceNonNullValueIsReturnedItIsAlwaysReturned(boolean validating) {
        setup(validating);
        AtomicTable<Long, TransactionStatus> putUnlessExistsTable = new ResilientCommitTimestampAtomicTable(
                new PueCassImitatingConsensusForgettingStore(0.5d),
                encodingStrategy,
                new DefaultTaggedMetricRegistry());

        for (long startTs = 1L; startTs < 1000; startTs++) {
            long ts = startTs;
            List<Long> successfulCommitTs = IntStream.range(0, 3)
                    .mapToObj(offset -> tryPue(putUnlessExistsTable, ts, ts + offset))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());
            assertThat(successfulCommitTs).hasSizeLessThanOrEqualTo(1);

            Optional<Long> onlyAllowedCommitTs = successfulCommitTs.stream().findFirst();
            for (int i = 0; i < 30; i++) {
                TransactionStatus valueRead = firstSuccessfulRead(putUnlessExistsTable, startTs);
                onlyAllowedCommitTs.ifPresentOrElse(
                        commit -> assertThat(valueRead).isEqualTo(TransactionStatus.committed(commit)),
                        () -> assertThat(valueRead)
                                .satisfiesAnyOf(
                                        status -> assertThat(status).isEqualTo(TransactionStatus.inProgress()),
                                        status -> assertThat(status).isEqualTo(TransactionStatus.committed(ts)),
                                        status -> assertThat(status).isEqualTo(TransactionStatus.committed(ts + 1)),
                                        status -> assertThat(status).isEqualTo(TransactionStatus.committed(ts + 2))));
                onlyAllowedCommitTs = TransactionStatus.getCommitTimestamp(valueRead);
            }
        }
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @ValueSource(booleans = {true, false})
    public void inAbsenceOfConcurrencyGetRetriesBothTouchAndPut(boolean validating)
            throws ExecutionException, InterruptedException {
        setup(validating);
        setupStagingValues(1);

        int numberOfReads = 100;
        for (int i = 0; i < numberOfReads; i++) {
            assertThatThrownBy(() -> atomicTable.get(0L).get())
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Failed to set value");
        }

        if (validating) {
            verify(spiedKvs, times(100)).checkAndSet(any());
        } else {
            verify(spiedKvs, never()).checkAndSet(any());
        }
        verify(spiedStore, times(101)).put(anyMap());

        spiedStore.stopFailingPuts();
        for (long i = 0; i < 100; i++) {
            assertThat(atomicTable.get(0L).get()).isEqualTo(TransactionStatus.committed(0L));
        }

        if (validating) {
            verify(spiedKvs, times(101)).checkAndSet(any());
        } else {
            verify(spiedKvs, never()).checkAndSet(any());
        }
        verify(spiedStore, times(102)).put(anyMap());
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @ValueSource(booleans = {true, false})
    public void noSuperfluousCasOrPuts(boolean validating) {
        setup(validating);
        setupStagingValues(50);

        spiedStore.stopFailingPuts();
        ExecutorService writers = Executors.newFixedThreadPool(100);
        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<Future<ListenableFuture<TransactionStatus>>> results = new ArrayList<>();
        int numberOfReads = 20_000;
        for (int i = 0; i < numberOfReads; i++) {
            results.add(writers.submit(() -> atomicTable.get(random.nextLong(50L))));
        }
        results.forEach(future -> assertThatCode(() -> future.get().get()).doesNotThrowAnyException());
        writers.shutdownNow();

        if (validating) {
            verify(spiedKvs, times(50)).checkAndSet(any());
        } else {
            verify(spiedKvs, never()).checkAndSet(any());
        }
        verify(spiedStore, times(1 + 50)).put(anyMap());
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @ValueSource(booleans = {true, false})
    public void touchesForSameRowAreSerial(boolean validating) {
        setup(validating);
        int rowsPerQuantum = TicketsEncodingStrategy.ROWS_PER_QUANTUM;
        int parallelism = 100;
        setupStagingValues(rowsPerQuantum * 10);

        ExecutorService writers = Executors.newFixedThreadPool(parallelism);
        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<Future<?>> results = new ArrayList<>();
        int numberOfReads = parallelism * 100;
        for (int i = 0; i < numberOfReads; i++) {
            // all timestamps go into the same row
            results.add(Futures.submitAsync(() -> atomicTable.get(random.nextLong(10) * rowsPerQuantum), writers));
        }
        results.forEach(future -> assertThatThrownBy(future::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(RuntimeException.class));
        spiedStore.stopFailingPuts();
        results.clear();
        for (int i = 0; i < numberOfReads; i++) {
            results.add(Futures.submitAsync(() -> atomicTable.get(random.nextLong(10) * rowsPerQuantum), writers));
        }
        results.forEach(future -> assertThatCode(future::get).doesNotThrowAnyException());
        writers.shutdownNow();

        assertThat(spiedStore.maximumConcurrentTouches()).isEqualTo(validating ? 1 : 0);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @ValueSource(booleans = {true, false})
    public void allowParallelTouchesForDifferentRows(boolean validating) {
        setup(validating);
        int maximumParallelism = TicketsEncodingStrategy.ROWS_PER_QUANTUM;
        setupStagingValues(maximumParallelism * 2);

        ExecutorService writers = Executors.newFixedThreadPool(maximumParallelism * 10);
        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<ListenableFuture<?>> results = new ArrayList<>();
        int numberOfReads = maximumParallelism * 100;
        for (int i = 0; i < numberOfReads; i++) {
            results.add(Futures.submitAsync(() -> atomicTable.get(random.nextLong(maximumParallelism * 2)), writers));
        }
        results.forEach(future -> assertThatThrownBy(future::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(RuntimeException.class));
        spiedStore.stopFailingPuts();
        results.clear();
        for (int i = 0; i < numberOfReads; i++) {
            results.add(Futures.submitAsync(() -> atomicTable.get(random.nextLong(maximumParallelism * 2)), writers));
        }
        results.forEach(future -> assertThatCode(future::get).doesNotThrowAnyException());
        writers.shutdownNow();

        if (validating) {
            assertThat(spiedStore.maximumConcurrentTouches()).isBetween(2, maximumParallelism);
        } else {
            assertThat(spiedStore.maximumConcurrentTouches()).isEqualTo(0);
        }
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @ValueSource(booleans = {true, false})
    public void doNotPutIfAlreadyCommitted(boolean validating) throws ExecutionException, InterruptedException {
        setup(validating);
        // not worth the effort to make this work
        Assumptions.assumeTrue(validating);
        setupStagingValues(1);
        spiedStore.enableCommittingUnderUs();

        assertThat(atomicTable.get(0L).get()).isEqualTo(TransactionStatus.committed(0L));
        verify(spiedKvs, times(1)).checkAndSet(any());
        // only the put from the original PUE was registered
        verify(spiedStore, times(1)).put(anyMap());
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @ValueSource(booleans = {true, false})
    public void acceptStagingAsCommittedWhenCommittingIsSlow(boolean validating)
            throws ExecutionException, InterruptedException {
        setup(validating);
        Assumptions.assumeTrue(validating);
        setupStagingValues(5);
        spiedStore.stopFailingPuts();
        spiedStore.startSlowPue();

        assertThat(atomicTable.get(0L).get()).isEqualTo(TransactionStatus.committed(0L));
        assertThat(atomicTable.get(1L).get()).isEqualTo(TransactionStatus.committed(1L));

        verify(spiedKvs, times(1)).checkAndSet(any());
        verify(spiedStore, times(1 + 2)).put(anyMap());

        clockLong.accumulateAndGet(Duration.ofSeconds(62).toMillis(), Long::sum);

        assertThat(atomicTable.get(2L).get()).isEqualTo(TransactionStatus.committed(2L));
        verify(spiedKvs, times(2)).checkAndSet(any());
        verify(spiedStore, times(1 + 3)).put(anyMap());
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @ValueSource(booleans = {true, false})
    public void acceptStagingAsCommittedWhenRetryingTooMuch(boolean validating)
            throws ExecutionException, InterruptedException {
        setup(validating);
        Assumptions.assumeTrue(validating);
        setupStagingValues(5);
        spiedStore.failPutsWithAtlasdbDependencyException();

        assertThatThrownBy(() -> atomicTable.get(0L).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(RetryLimitReachedException.class);
        spiedStore.stopFailingPuts();

        assertThat(atomicTable.get(1L).get()).isEqualTo(TransactionStatus.committed(1L));
        verify(spiedKvs, times(1)).checkAndSet(any());
        verify(spiedStore, times(1 + 2)).put(anyMap());

        clockLong.accumulateAndGet(Duration.ofSeconds(62).toMillis(), Long::sum);

        assertThat(atomicTable.get(2L).get()).isEqualTo(TransactionStatus.committed(2L));
        verify(spiedKvs, times(2)).checkAndSet(any());
        verify(spiedStore, times(1 + 3)).put(anyMap());
    }

    public void setup(boolean validating) {
        atomicTable = new ResilientCommitTimestampAtomicTable(
                spiedStore, encodingStrategy, () -> !validating, clock, new DefaultTaggedMetricRegistry());
    }

    private void setupStagingValues(int num) {
        spiedStore.startFailingPuts();
        Map<Long, TransactionStatus> initialWrites =
                LongStream.range(0, num).boxed().collect(Collectors.toMap(x -> x, TransactionStatus::committed));
        assertThatThrownBy(() -> atomicTable.updateMultiple(initialWrites))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to set value");
    }

    private static Optional<Long> tryPue(
            AtomicTable<Long, TransactionStatus> atomicTable, long startTs, long commitTs) {
        try {
            atomicTable.update(startTs, TransactionStatus.committed(commitTs));
            return Optional.of(commitTs);
        } catch (Exception e) {
            // this is ok, we may have failed because it already exists or randomly. Either way, continue.
            return Optional.empty();
        }
    }

    private static TransactionStatus firstSuccessfulRead(AtomicTable<Long, TransactionStatus> atomicTable, long ts) {
        while (true) {
            try {
                return atomicTable.get(ts).get();
            } catch (Exception e) {
                // this is ok, when we try to read we may end up doing a write, which can throw -- we will retry
            }
        }
    }

    /**
     * An implementation of the consensus forgetting store that allows us to simulate failures after the atomic
     * operation in the resilient PUE table protocol, and inspect the concurrency guarantees for the touch method.
     * <p>
     * WARNING: the usefulness of this store is coupled with the implementation of
     * {@link PueConsensusForgettingStore} and {@link ResilientCommitTimestampAtomicTable}. If implementation
     * details are changed, it may invalidate tests relying on this class.
     */
    private class UnreliablePueConsensusForgettingStore extends PueConsensusForgettingStore {
        private volatile Optional<RuntimeException> putException = Optional.empty();
        private final AtomicInteger concurrentTouches = new AtomicInteger(0);
        private final AtomicInteger maximumConcurrentTouches = new AtomicInteger(0);
        private volatile boolean commitUnderUs = false;
        private volatile long millisForPue = 0;

        public UnreliablePueConsensusForgettingStore(KeyValueService kvs, TableReference tableRef) {
            super(kvs, tableRef);
        }

        @Override
        public void put(Map<Cell, byte[]> values) {
            if (putException.isPresent()) {
                throw putException.get();
            }
            super.put(values);
        }

        /**
         * We rely on the fact that {@link PueConsensusForgettingStore} uses the default
         * implementation of {@link ConsensusForgettingStore#checkAndTouch(Map)}
         */
        @Override
        public void checkAndTouch(Cell cell, byte[] value) throws CheckAndSetException {
            if (commitUnderUs) {
                super.put(ImmutableMap.of(cell, encodingStrategy.transformStagingToCommitted(value)));
            }
            int current = concurrentTouches.incrementAndGet();
            if (current > maximumConcurrentTouches.get()) {
                maximumConcurrentTouches.accumulateAndGet(current, Math::max);
            }
            super.checkAndTouch(cell, value);
            clockLong.getAndAccumulate(millisForPue, Long::sum);
            concurrentTouches.decrementAndGet();
        }

        /**
         * This effectively causes all newly put values to be stuck in staging
         */
        public void startFailingPuts() {
            putException = Optional.of(new RuntimeException("Failed to set value"));
        }

        public void stopFailingPuts() {
            putException = Optional.empty();
        }

        public void failPutsWithAtlasdbDependencyException() {
            putException = Optional.of(new RetryLimitReachedException(ImmutableList.of(), ImmutableMap.of("host1", 1)));
        }

        public int maximumConcurrentTouches() {
            return maximumConcurrentTouches.get();
        }

        public void startSlowPue() {
            millisForPue = Duration.ofSeconds(2).toMillis();
        }

        /**
         * This will cause a staging value to be committed just before we try to touch it
         */
        public void enableCommittingUnderUs() {
            commitUnderUs = true;
        }
    }
}
