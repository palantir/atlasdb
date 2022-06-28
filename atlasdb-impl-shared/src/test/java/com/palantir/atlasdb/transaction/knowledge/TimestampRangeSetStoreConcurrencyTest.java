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
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.google.common.collect.Range;
import com.palantir.atlasdb.keyvalue.api.AutoDelegate_KeyValueService;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.concurrent.PTExecutors;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.Test;

@SuppressWarnings("UnstableApiUsage") // RangeSet usage
public class TimestampRangeSetStoreConcurrencyTest {
    private final AtomicBoolean blockCalls = new AtomicBoolean(false);
    private final CountDownLatch latch = new CountDownLatch(1);

    private final KeyValueService delegateKeyValueService = spy(new InMemoryKeyValueService(true));
    private final KeyValueService blockingKeyValueService = (AutoDelegate_KeyValueService) () -> {
        if (blockCalls.get()) {
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        return delegateKeyValueService;
    };
    private final TimestampRangeSetStore timestampRangeSetStore =
            TimestampRangeSetStore.create(blockingKeyValueService);
    private final ExecutorService taskExecutor = PTExecutors.newCachedThreadPool();

    @Test
    public void batchesReadsUnderHighConcurrency() throws InterruptedException {
        timestampRangeSetStore.supplement(Range.closedOpen(10L, 50L));

        startBlockingKeyValueServiceCalls();
        int numThreads = 100;

        List<Future<Optional<TimestampRangeSet>>> readFutures = new ArrayList<>(numThreads);
        for (int i = 0; i < numThreads; i++) {
            Future<Optional<TimestampRangeSet>> rangeSetFuture = taskExecutor.submit(timestampRangeSetStore::get);
            readFutures.add(rangeSetFuture);
        }

        List<Optional<TimestampRangeSet>> reads = letTasksRunToCompletion(readFutures, false);
        for (Optional<TimestampRangeSet> read : reads) {
            assertThat(read).contains(TimestampRangeSet.singleRange(Range.closedOpen(10L, 50L)));
        }

        verify(delegateKeyValueService, atMost(10))
                .get(eq(TransactionConstants.KNOWN_CONCLUDED_TRANSACTIONS_TABLE), anyMap());
    }

    @Test
    public void writesPreserveCorrectnessUnderHighConcurrency() throws InterruptedException {
        startBlockingKeyValueServiceCalls();

        int numThreads = 300;
        List<Range<Long>> candidateTimestampRanges = LongStream.range(0, numThreads)
                .mapToObj(index -> Range.closed(2 * index, 2 * index + 1))
                .collect(Collectors.toList());

        List<Future<Void>> supplementFutures = new ArrayList<>(numThreads);
        for (int thread = 0; thread < numThreads; thread++) {
            int threadIndex = thread;
            Future<Void> supplementFuture = taskExecutor.submit(() -> {
                timestampRangeSetStore.supplement(candidateTimestampRanges.get(threadIndex));
                return null;
            });
            supplementFutures.add(supplementFuture);
        }

        letTasksRunToCompletion(supplementFutures, true);

        Optional<TimestampRangeSet> rangesInDb = timestampRangeSetStore.get();
        assertThat(rangesInDb).hasValueSatisfying(timestampRangeSet -> assertThat(
                        timestampRangeSet.timestampRanges().asRanges())
                .isSubsetOf(candidateTimestampRanges));
    }

    @Test
    public void writesOfMostlySimilarRangesAreCoalescedCorrectly() throws InterruptedException {
        startBlockingKeyValueServiceCalls();

        int numThreads = 300;
        int threadsPerRange = 25;
        List<Range<Long>> candidateTimestampRanges = LongStream.range(0, numThreads / threadsPerRange)
                .mapToObj(index -> Range.closed(2 * index, 2 * index + 1))
                .collect(Collectors.toList());

        List<Future<Void>> supplementFutures = new ArrayList<>(numThreads);
        for (int thread = 0; thread < numThreads; thread++) {
            int threadIndex = thread;
            Future<Void> supplementFuture = taskExecutor.submit(() -> {
                timestampRangeSetStore.supplement(candidateTimestampRanges.get(threadIndex / threadsPerRange));
                return null;
            });
            supplementFutures.add(supplementFuture);
        }

        letTasksRunToCompletion(supplementFutures, true);

        Optional<TimestampRangeSet> rangesInDb = timestampRangeSetStore.get();
        assertThat(rangesInDb).hasValueSatisfying(timestampRangeSet -> assertThat(
                        timestampRangeSet.timestampRanges().asRanges())
                .as("Given similarity of ranges, concurrency should be handled smoothly")
                .hasSameElementsAs(candidateTimestampRanges));
    }

    private void startBlockingKeyValueServiceCalls() {
        blockCalls.set(true);
    }

    private <T> List<T> letTasksRunToCompletion(List<Future<T>> taskFutures, boolean failuresPermitted)
            throws InterruptedException {
        latch.countDown();
        taskExecutor.shutdown();
        taskExecutor.awaitTermination(5, TimeUnit.SECONDS);

        List<T> resultAccumulator = new ArrayList<>();
        for (Future<T> taskFuture : taskFutures) {
            try {
                resultAccumulator.add(taskFuture.get());
            } catch (ExecutionException e) {
                if (!failuresPermitted) {
                    Throwable throwable = e.getCause();
                    if (throwable instanceof RuntimeException) {
                        throw (RuntimeException) throwable;
                    }
                    throw new RuntimeException(throwable);
                }
            }
        }
        return resultAccumulator;
    }
}
