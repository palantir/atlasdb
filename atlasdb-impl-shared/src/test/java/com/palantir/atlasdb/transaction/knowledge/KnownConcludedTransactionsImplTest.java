/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.knowledge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.transaction.knowledge.KnownConcludedTransactions.Consistency;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;

public class KnownConcludedTransactionsImplTest {
    private static final Range<Long> DEFAULT_RANGE = Range.closed(10L, 100L);
    private static final Range<Long> ADDITIONAL_RANGE = Range.closed(88L, 200L);

    private final TaggedMetricRegistry taggedMetricRegistry = new DefaultTaggedMetricRegistry();
    private final KnownConcludedTransactionsStore knownConcludedTransactionsStore =
            mock(KnownConcludedTransactionsStore.class);
    private final KnownConcludedTransactions defaultKnownConcludedTransactions =
            KnownConcludedTransactionsImpl.create(knownConcludedTransactionsStore, taggedMetricRegistry);

    @Before
    public void setUp() {
        setupStoreWithDefaultRange();
    }

    @Test
    public void isKnownConcludedDoesNotPerformDatabaseReadsOnLocalReadConsistency() {
        assertThat(defaultKnownConcludedTransactions.isKnownConcluded(2L, Consistency.LOCAL_READ))
                .isFalse();
        assertThat(defaultKnownConcludedTransactions.isKnownConcluded(84L, Consistency.LOCAL_READ))
                .isFalse();

        verify(knownConcludedTransactionsStore, never()).get();
    }

    @Test
    public void isKnownConcludedPerformsDatabaseReadOnRemoteReadConsistencyAndCanReturnInconclusive() {
        assertThat(defaultKnownConcludedTransactions.isKnownConcluded(2L, Consistency.REMOTE_READ))
                .isFalse();
        verify(knownConcludedTransactionsStore).get();
    }

    @Test
    public void isKnownConcludedPerformsDatabaseReadOnRemoteReadConsistencyAndCanReturnConcluded() {
        assertThat(defaultKnownConcludedTransactions.isKnownConcluded(85L, Consistency.REMOTE_READ))
                .isTrue();
        verify(knownConcludedTransactionsStore).get();
    }

    @Test
    public void isKnownConcludedDoesNotPerformDatabaseReadIfAnswerAlreadyKnown() {
        assertThat(defaultKnownConcludedTransactions.isKnownConcluded(85L, Consistency.REMOTE_READ))
                .isTrue();
        verify(knownConcludedTransactionsStore).get();
        assertThat(defaultKnownConcludedTransactions.isKnownConcluded(47L, Consistency.REMOTE_READ))
                .isTrue();
        assertThat(defaultKnownConcludedTransactions.isKnownConcluded(62L, Consistency.REMOTE_READ))
                .isTrue();
        assertThat(defaultKnownConcludedTransactions.isKnownConcluded(
                        DEFAULT_RANGE.upperEndpoint(), Consistency.REMOTE_READ))
                .isTrue();
        verifyNoMoreInteractions(knownConcludedTransactionsStore);
    }

    @Test
    public void isKnownConcludedAttemptsToLoadNewInformationIfAnswerNotCurrentlyKnown() {
        // In each instance, it is possible that external factors have modified the database so that the transaction
        // becomes known to have concluded, so a remote read is needed on each attempt.
        assertThat(defaultKnownConcludedTransactions.isKnownConcluded(2L, Consistency.REMOTE_READ))
                .isFalse();
        assertThat(defaultKnownConcludedTransactions.isKnownConcluded(2L, Consistency.REMOTE_READ))
                .isFalse();
        assertThat(defaultKnownConcludedTransactions.isKnownConcluded(2L, Consistency.REMOTE_READ))
                .isFalse();
        verify(knownConcludedTransactionsStore, times(3)).get();

        when(knownConcludedTransactionsStore.get())
                .thenReturn(Optional.of(TimestampRangeSet.singleRange(Range.closed(0L, 100L), 0L)));
        assertThat(defaultKnownConcludedTransactions.isKnownConcluded(2L, Consistency.REMOTE_READ))
                .isTrue();
        verify(knownConcludedTransactionsStore, times(4)).get();
    }

    @Test
    public void addConcludedTimestampsSupplementsUnderlyingStore() {
        defaultKnownConcludedTransactions.addConcludedTimestamps(ADDITIONAL_RANGE);
        verify(knownConcludedTransactionsStore).supplement(ImmutableSet.of(ADDITIONAL_RANGE));
    }

    @Test
    public void addConcludedTimestampsThrowsIfRangeNotClosed() {
        assertThatThrownBy(() -> defaultKnownConcludedTransactions.addConcludedTimestamps(Range.atLeast(1L)))
                .isInstanceOf(SafeIllegalStateException.class);

        assertThatThrownBy(() -> defaultKnownConcludedTransactions.addConcludedTimestamps(Range.atMost(100L)))
                .isInstanceOf(SafeIllegalStateException.class);

        assertThatThrownBy(() -> defaultKnownConcludedTransactions.addConcludedTimestamps(Range.openClosed(1L, 100L)))
                .isInstanceOf(SafeIllegalStateException.class);

        assertThatThrownBy(() -> defaultKnownConcludedTransactions.addConcludedTimestamps(Range.closedOpen(1L, 100L)))
                .isInstanceOf(SafeIllegalStateException.class);

        verifyNoMoreInteractions(knownConcludedTransactionsStore);
    }

    @Test
    public void metricPublishesNumberOfDisjointCachedRanges() {
        when(knownConcludedTransactionsStore.get())
                .thenReturn(Optional.of(TimestampRangeSet.singleRange(Range.closed(0L, 100L), 0L)));
        assertThat(defaultKnownConcludedTransactions.isKnownConcluded(2L, Consistency.REMOTE_READ))
                .isTrue();
        assertDisjointCacheIntervalMetricIsPublishedAndHasValue(1);

        defaultKnownConcludedTransactions.addConcludedTimestamps(Range.closed(150L, 250L));
        defaultKnownConcludedTransactions.addConcludedTimestamps(Range.closed(350L, 450L));
        assertDisjointCacheIntervalMetricIsPublishedAndHasValue(3);

        defaultKnownConcludedTransactions.addConcludedTimestamps(Range.closed(0L, 888L));
        assertDisjointCacheIntervalMetricIsPublishedAndHasValue(1);
    }

    @Test
    public void addConcludedTimestampsFlushesWritesToTheCache() {
        defaultKnownConcludedTransactions.addConcludedTimestamps(ADDITIONAL_RANGE);
        assertThat(defaultKnownConcludedTransactions.isKnownConcluded(
                        DEFAULT_RANGE.upperEndpoint() + 1, Consistency.LOCAL_READ))
                .isTrue();
        assertThat(defaultKnownConcludedTransactions.isKnownConcluded(
                        DEFAULT_RANGE.upperEndpoint() + 1, Consistency.REMOTE_READ))
                .isTrue();
        verify(knownConcludedTransactionsStore, never()).get();
    }

    @Test
    public void lastLocallyKnownConcludedTimestampRequestDoesNotHitRemote() {
        assertThat(defaultKnownConcludedTransactions.lastLocallyKnownConcludedTimestamp())
                .isEqualTo(0);
        verify(knownConcludedTransactionsStore, never()).get();
    }

    @Test
    public void canGetLastLocallyKnownConcludedTimestamp() {
        defaultKnownConcludedTransactions.addConcludedTimestamps(ADDITIONAL_RANGE);
        assertThat(defaultKnownConcludedTransactions.lastLocallyKnownConcludedTimestamp())
                .isEqualTo(ADDITIONAL_RANGE.upperEndpoint());
    }

    @Test
    public void addConcludedTimestampsConcurrency() throws InterruptedException {
        int numThreads = 10; // Concurrency should not be too high given this is accessed from background threads.
        CountDownLatch startTaskLatch = new CountDownLatch(1);
        ExecutorService taskExecutor = Executors.newCachedThreadPool();

        List<Future<Void>> futures = IntStream.range(0, numThreads)
                .mapToObj(index -> taskExecutor.submit((Callable<Void>) () -> {
                    try {
                        startTaskLatch.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    defaultKnownConcludedTransactions.addConcludedTimestamps(
                            Range.closed(index * 10L, index * 10L + 9));
                    return null;
                }))
                .collect(Collectors.toList());

        startTaskLatch.countDown();
        taskExecutor.shutdown();
        taskExecutor.awaitTermination(5, TimeUnit.SECONDS);
        futures.forEach(Futures::getUnchecked);

        IntStream.range(0, numThreads * 10).forEach(timestamp -> assertThat(
                        defaultKnownConcludedTransactions.isKnownConcluded(timestamp, Consistency.LOCAL_READ))
                .isTrue());
    }

    private void setupStoreWithDefaultRange() {
        when(knownConcludedTransactionsStore.get())
                .thenReturn(Optional.of(TimestampRangeSet.singleRange(DEFAULT_RANGE, 0L)));
    }

    private void assertDisjointCacheIntervalMetricIsPublishedAndHasValue(int expected) {
        assertThat(taggedMetricRegistry.gauge(KnownConcludedTransactionsMetrics.disjointCacheIntervalsMetricName()))
                .isPresent()
                .hasValueSatisfying(gauge -> assertThat(gauge.getValue()).isEqualTo(expected));
    }
}
