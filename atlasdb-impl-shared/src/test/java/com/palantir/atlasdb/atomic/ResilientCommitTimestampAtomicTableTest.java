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
import com.palantir.atlasdb.transaction.service.TransactionStatuses;
import com.palantir.common.time.Clock;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ResilientCommitTimestampAtomicTableTest {
    private static final String VALIDATING_STAGING_VALUES = "validating staging values";
    private static final String NOT_VALIDATING_STAGING_VALUES = "not validating staging values";

    private final KeyValueService spiedKvs = spy(new InMemoryKeyValueService(true));
    private final UnreliablePueKvsConsensusForgettingStore spiedStore =
            spy(new UnreliablePueKvsConsensusForgettingStore(
                    spiedKvs, TableReference.createFromFullyQualifiedName("test.table")));

    private final boolean validating;
    private final AtomicTable<Long, TransactionStatus> atomicTable;
    private final AtomicLong clockLong = new AtomicLong(1000);
    private final Clock clock = clockLong::get;
    private final TwoPhaseEncodingStrategy encodingStrategy =
            new TwoPhaseEncodingStrategy(BaseProgressEncodingStrategy.INSTANCE);

    public ResilientCommitTimestampAtomicTableTest(String name, Object parameter) {
        validating = (boolean) parameter;
        atomicTable = new ResilientCommitTimestampAtomicTable(
                spiedStore, encodingStrategy, () -> !validating, clock, new DefaultTaggedMetricRegistry());
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        Object[][] data = new Object[][] {
            {VALIDATING_STAGING_VALUES, true},
            {NOT_VALIDATING_STAGING_VALUES, false}
        };
        return Arrays.asList(data);
    }

    @Test
    public void canPutAndGet() throws ExecutionException, InterruptedException {
        atomicTable.update(1L, TransactionStatuses.committed(2L));
        assertThat(TransactionStatuses.getCommitTimestamp(atomicTable.get(1L).get()))
                .hasValue(2L);
        verify(spiedStore).atomicUpdate(anyMap());
        verify(spiedStore, atLeastOnce()).put(anyMap());
        verify(spiedStore).getMultiple(any());
    }

    @Test
    public void emptyReturnsInProgress() throws ExecutionException, InterruptedException {
        assertThat(TransactionStatuses.caseOf(atomicTable.get(3L).get())
                        .inProgress_(true)
                        .otherwise_(false))
                .isTrue();
    }

    @Test
    public void canPutAndGetAbortedTransactions() throws ExecutionException, InterruptedException {
        atomicTable.update(1L, TransactionStatuses.aborted());
        assertThat(TransactionStatuses.caseOf(atomicTable.get(1L).get())
                        .aborted_(true)
                        .otherwise_(false))
                .isTrue();
    }

    @Test
    public void cannotPueTwice() {
        atomicTable.update(1L, TransactionStatuses.committed(2L));
        assertThatThrownBy(() -> atomicTable.update(1L, TransactionStatuses.committed(2L)))
                .isInstanceOf(KeyAlreadyExistsException.class);
    }

    @Test
    public void canPutAndGetMultiple() throws ExecutionException, InterruptedException {
        ImmutableMap<Long, TransactionStatus> inputs = ImmutableMap.of(
                1L,
                TransactionStatuses.committed(2L),
                3L,
                TransactionStatuses.committed(4L),
                7L,
                TransactionStatuses.committed(8L));
        atomicTable.updateMultiple(inputs);
        Map<Long, TransactionStatus> result =
                atomicTable.get(ImmutableList.of(1L, 3L, 5L, 7L)).get();
        assertThat(result.size()).isEqualTo(4);
        assertThat(TransactionStatuses.getCommitTimestamp(result.get(1L))).hasValue(2L);
        assertThat(TransactionStatuses.getCommitTimestamp(result.get(3L))).hasValue(4L);
        assertThat(TransactionStatuses.caseOf(result.get(5L)).inProgress_(true).otherwise_(false))
                .isTrue();
        assertThat(TransactionStatuses.getCommitTimestamp(result.get(7L))).hasValue(8L);
    }

    @Test
    public void pueThatThrowsIsCorrectedOnGet() throws ExecutionException, InterruptedException {
        spiedStore.startFailingPuts();
        assertThatThrownBy(() -> atomicTable.update(1L, TransactionStatuses.committed(2L)))
                .isInstanceOf(RuntimeException.class);
        spiedStore.stopFailingPuts();

        assertThat(TransactionStatuses.getCommitTimestamp(atomicTable.get(1L).get()))
                .hasValue(2L);
        verify(spiedStore, times(2)).put(anyMap());
    }

    @Test
    public void getReturnsStagingValuesThatWereCommittedBySomeoneElse()
            throws ExecutionException, InterruptedException {

        long startTimestamp = 1L;
        TransactionStatus commitStatus = TransactionStatuses.committed(2L);
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

        assertThat(TransactionStatuses.getCommitTimestamp(
                        atomicTable.get(startTimestamp).get()))
                .isEqualTo(TransactionStatuses.getCommitTimestamp(commitStatus));
    }

    @Test
    public void onceNonNullValueIsReturnedItIsAlwaysReturned() {
        AtomicTable<Long, TransactionStatus> putUnlessExistsTable = new ResilientCommitTimestampAtomicTable(
                new CassandraImitatingConsensusForgettingStore(0.5d),
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
                        commit -> assertThat(TransactionStatuses.getCommitTimestamp(valueRead))
                                .hasValue(commit),
                        () -> assertThat(TransactionStatuses.caseOf(valueRead)
                                        .inProgress_(true)
                                        .committed(commitTs -> commitTs >= ts && commitTs <= ts + 2)
                                        .otherwise_(false))
                                .isTrue());
                onlyAllowedCommitTs = TransactionStatuses.getCommitTimestamp(valueRead);
            }
        }
    }

    @Test
    public void inAbsenceOfConcurrencyGetRetriesBothTouchAndPut() throws ExecutionException, InterruptedException {
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
            assertThat(TransactionStatuses.getCommitTimestamp(
                            atomicTable.get(0L).get()))
                    .hasValue(0L);
        }

        if (validating) {
            verify(spiedKvs, times(101)).checkAndSet(any());
        } else {
            verify(spiedKvs, never()).checkAndSet(any());
        }
        verify(spiedStore, times(102)).put(anyMap());
    }

    @Test
    public void noSuperfluousCasOrPuts() {
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

    @Test
    public void touchesForSameRowAreSerial() {
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

    @Test
    public void allowParallelTouchesForDifferentRows() {
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

    @Test
    public void doNotPutIfAlreadyCommitted() throws ExecutionException, InterruptedException {
        // not worth the effort to make this work
        Assume.assumeTrue(validating);
        setupStagingValues(1);
        spiedStore.enableCommittingUnderUs();

        assertThat(TransactionStatuses.getCommitTimestamp(atomicTable.get(0L).get()))
                .hasValue(0L);
        verify(spiedKvs, times(1)).checkAndSet(any());
        // only the put from the original PUE was registered
        verify(spiedStore, times(1)).put(anyMap());
    }

    @Test
    public void acceptStagingAsCommittedWhenCommittingIsSlow() throws ExecutionException, InterruptedException {
        Assume.assumeTrue(validating);
        setupStagingValues(5);
        spiedStore.stopFailingPuts();
        spiedStore.startSlowPue();

        assertThat(TransactionStatuses.getCommitTimestamp(atomicTable.get(0L).get()))
                .hasValue(0L);
        assertThat(TransactionStatuses.getCommitTimestamp(atomicTable.get(1L).get()))
                .hasValue(1L);

        verify(spiedKvs, times(1)).checkAndSet(any());
        verify(spiedStore, times(1 + 2)).put(anyMap());

        clockLong.accumulateAndGet(Duration.ofSeconds(62).toMillis(), Long::sum);

        assertThat(TransactionStatuses.getCommitTimestamp(atomicTable.get(2L).get()))
                .hasValue(2L);
        verify(spiedKvs, times(2)).checkAndSet(any());
        verify(spiedStore, times(1 + 3)).put(anyMap());
    }

    @Test
    public void acceptStagingAsCommittedWhenRetryingTooMuch() throws ExecutionException, InterruptedException {
        Assume.assumeTrue(validating);
        setupStagingValues(5);
        spiedStore.failPutsWithAtlasdbDependencyException();

        assertThatThrownBy(() -> atomicTable.get(0L).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(RetryLimitReachedException.class);
        spiedStore.stopFailingPuts();

        assertThat(TransactionStatuses.getCommitTimestamp(atomicTable.get(1L).get()))
                .hasValue(1L);
        verify(spiedKvs, times(1)).checkAndSet(any());
        verify(spiedStore, times(1 + 2)).put(anyMap());

        clockLong.accumulateAndGet(Duration.ofSeconds(62).toMillis(), Long::sum);

        assertThat(TransactionStatuses.getCommitTimestamp(atomicTable.get(2L).get()))
                .hasValue(2L);
        verify(spiedKvs, times(2)).checkAndSet(any());
        verify(spiedStore, times(1 + 3)).put(anyMap());
    }

    private void setupStagingValues(int num) {
        spiedStore.startFailingPuts();
        Map<Long, TransactionStatus> initialWrites =
                LongStream.range(0, num).boxed().collect(Collectors.toMap(x -> x, TransactionStatuses::committed));
        assertThatThrownBy(() -> atomicTable.updateMultiple(initialWrites))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to set value");
    }

    private static Optional<Long> tryPue(
            AtomicTable<Long, TransactionStatus> atomicTable, long startTs, long commitTs) {
        try {
            atomicTable.update(startTs, TransactionStatuses.committed(commitTs));
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
     *
     * WARNING: the usefulness of this store is coupled with the implementation of
     * {@link PueKvsConsensusForgettingStore} and {@link ResilientCommitTimestampAtomicTable}. If implementation
     * details are changed, it may invalidate tests relying on this class.
     */
    private class UnreliablePueKvsConsensusForgettingStore extends PueKvsConsensusForgettingStore {
        private volatile Optional<RuntimeException> putException = Optional.empty();
        private final AtomicInteger concurrentTouches = new AtomicInteger(0);
        private final AtomicInteger maximumConcurrentTouches = new AtomicInteger(0);
        private volatile boolean commitUnderUs = false;
        private volatile long millisForPue = 0;

        public UnreliablePueKvsConsensusForgettingStore(KeyValueService kvs, TableReference tableRef) {
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
         * We rely on the fact that {@link PueKvsConsensusForgettingStore} uses the default
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
            putException = Optional.of(new RetryLimitReachedException(ImmutableList.of()));
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
