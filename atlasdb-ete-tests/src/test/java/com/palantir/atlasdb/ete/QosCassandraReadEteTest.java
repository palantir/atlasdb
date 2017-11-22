/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.ete;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.qos.ratelimit.RateLimitExceededException;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoSchema;
import com.palantir.common.base.BatchingVisitable;

public class QosCassandraReadEteTest extends QosCassandraTestSetup {

    private static final int ONE_TODO_SIZE_IN_BYTES = 1050;

    @BeforeClass
    public static void createTransactionManagerAndWriteData() {
        ensureTransactionManagerIsCreated();
        writeNTodosOfSize(200, 1_000);
        serializableTransactionManager.close();
    }

    @Test
    public void shouldBeAbleToReadSmallAmountOfBytesIfDoesNotExceedLimit() {
        assertThat(readOneBatchOfSize(1)).hasSize(1);
    }

    @Test
    public void shouldBeAbleToReadSmallAmountOfBytesSeriallyIfDoesNotExceedLimit() {
        IntStream.range(0, 50).forEach(i -> assertThat(readOneBatchOfSize(1)).hasSize(1));
    }

    @Test
    public void shouldBeAbleToReadLargeAmountsExceedingTheLimitFirstTime() {
        assertThat(readOneBatchOfSize(12)).hasSize(12);
    }

    @Test
    public void shouldBeAbleToReadLargeAmountsExceedingTheLimitSecondTimeWithSoftLimiting() {
        assertThat(readOneBatchOfSize(12)).hasSize(12);
        // The second read might actually be faster as the transaction/metadata
        assertThat(readOneBatchOfSize(12)).hasSize(12);
    }

    @Test
    public void shouldNotBeAbleToReadLargeAmountsIfSoftLimitSleepWillBeMoreThanConfiguredBackoffTime() {
        assertThatThrownBy(() -> readOneBatchOfSize(200))
                .isInstanceOf(RateLimitExceededException.class)
                .hasMessage("Rate limited. Available capacity has been exhausted.");
    }

    @Test
    public void readRateLimitShouldBeRespectedByConcurrentReadingThreads() throws InterruptedException {
        int numThreads = 5;
        int numReadsPerThread = 10;
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        List<Future<List<Todo>>> futures = new ArrayList<>(numThreads);

        long start = System.nanoTime();
        IntStream.range(0, numThreads).forEach(i ->
                futures.add(executorService.submit(() -> {
                    List<Todo> results = new ArrayList<>(numReadsPerThread);
                    IntStream.range(0, numReadsPerThread)
                            .forEach(j -> results.addAll(readOneBatchOfSize(1)));
                    return results;
                })));
        executorService.shutdown();
        Preconditions.checkState(executorService.awaitTermination(30L, TimeUnit.SECONDS),
                "Read tasks did not finish in 30s");
        long timeTakenToReadInSeconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);

        assertThatAllReadsWereSuccessful(futures, numReadsPerThread);
        long actualReadRate = (numThreads * numReadsPerThread * ONE_TODO_SIZE_IN_BYTES) / timeTakenToReadInSeconds;
        assertThat(actualReadRate).isLessThan(readBytesPerSecond + (readBytesPerSecond / 10 /* to allow burst time */));
    }

    private void assertThatAllReadsWereSuccessful(List<Future<List<Todo>>> futures, int numReadsPerThread) {
        AtomicInteger exceptionCounter = new AtomicInteger(0);
        futures.forEach(future -> {
            try {
                assertThat(future.get()).hasSize(numReadsPerThread);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof RateLimitExceededException) {
                    exceptionCounter.getAndIncrement();
                }
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            }
        });
        assertThat(exceptionCounter.get()).isEqualTo(0);
    }

    private List<Todo> readOneBatchOfSize(int batchSize) {
        ImmutableList<RowResult<byte[]>> results = serializableTransactionManager.runTaskWithRetry((transaction) -> {
            BatchingVisitable<RowResult<byte[]>> rowResultBatchingVisitable = transaction.getRange(
                    TodoSchema.todoTable(), RangeRequest.all());
            ImmutableList.Builder<RowResult<byte[]>> rowResults = ImmutableList.builder();

            rowResultBatchingVisitable.batchAccept(batchSize, items -> {
                rowResults.addAll(items);
                return false;
            });

            return rowResults.build();
        });

        return results.stream()
                .map(RowResult::getOnlyColumnValue)
                .map(ValueType.STRING::convertToString)
                .map(ImmutableTodo::of)
                .collect(Collectors.toList());
    }

    @After
    public void cleanup() {
        serializableTransactionManager.close();
    }

}
