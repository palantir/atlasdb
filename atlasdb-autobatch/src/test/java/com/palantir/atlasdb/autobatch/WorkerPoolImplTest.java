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

package com.palantir.atlasdb.autobatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.awaitility.Awaitility;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public final class WorkerPoolImplTest {
    private static final String SAFE_LOGGABLE_PURPOSE = "test";

    @Test
    public void workerPoolCloseDelegatesToExecutorService() {
        List<Runnable> runnableList = List.of(() -> {});
        ExecutorService executorService = mock(ExecutorService.class);
        when(executorService.shutdownNow()).thenReturn(runnableList);
        WorkerPool workerPool = createSingleWorkerPool(executorService);
        assertThat(workerPool.close()).isEqualTo(runnableList);
    }

    @Test
    public void noOpWorkerPoolDoesNotRunTaskAndDoesNotConsumeSupplierAndReturnsFalseAndReturnsEmptyListOnClose() {
        WorkerPool workerPool = createWorkerPool(0);
        TestTaskSubmission taskSubmission = TestTaskSubmission.submitTask(workerPool);
        taskSubmission.assertTaskNotSubmitted();
        taskSubmission.assertSupplierWasNotConsumed();
        taskSubmission.assertTaskDidNotRun();
        assertThat(workerPool.close()).isEmpty();
    }

    @ValueSource(ints = {42, 78})
    @ParameterizedTest
    public void workerPoolRunsTaskWithResultFromSupplierWhenWorkerIsAvailable(int expectedResult) {
        DeterministicScheduler executorService = new DeterministicScheduler();
        WorkerPool workerPool = createSingleWorkerPool(executorService);
        TestTaskSubmission taskSubmission = TestTaskSubmission.submitTask(workerPool, expectedResult);
        taskSubmission.assertTaskSubmitted();
        executorService.runUntilIdle();
        taskSubmission.assertConsumedResultWas(expectedResult);
    }

    @Test
    public void workerPoolDoesNotConsumeSupplierIfAllWorkersAreBusy() {
        DeterministicScheduler executorService = new DeterministicScheduler();
        WorkerPool workerPool = createSingleWorkerPool(executorService);

        TestTaskSubmission firstTaskSubmission = TestTaskSubmission.submitTask(workerPool);
        firstTaskSubmission.assertTaskSubmitted();

        TestTaskSubmission secondTaskSubmission = TestTaskSubmission.submitTask(workerPool);
        secondTaskSubmission.assertTaskNotSubmitted();
        executorService.runUntilIdle();
        secondTaskSubmission.assertSupplierWasNotConsumed();
    }

    @Test
    public void workerPoolConsumesSupplierExactlyOnceAndInTheCallingThreadWhenWorkerIsAvailable() {
        DeterministicScheduler executorService = new DeterministicScheduler();
        WorkerPool workerPool = createSingleWorkerPool(executorService);
        TestTaskSubmission taskSubmission = TestTaskSubmission.submitTask(workerPool);
        taskSubmission.assertTaskSubmitted();
        taskSubmission.assertSupplierWasConsumedExactlyOnce();
        executorService.runUntilIdle();
        taskSubmission.assertTaskRan();
        taskSubmission.assertSupplierWasConsumedExactlyOnce();
    }

    @Test
    public void workerPoolDoesNotSubmitTaskAndReleasesWorkerResourceOnSupplierException() {
        DeterministicScheduler executorService = new DeterministicScheduler();
        WorkerPool workerPool = createSingleWorkerPool(executorService);
        TestTaskSubmission taskSubmissionWithThrowingSupplier =
                TestTaskSubmission.submitTaskWithThrowingSupplier(workerPool);

        taskSubmissionWithThrowingSupplier.assertTaskNotSubmitted();
        assertThat(executorService.isIdle()).isTrue();

        TestTaskSubmission taskSubmissionWithoutThrowingSupplier = TestTaskSubmission.submitTask(workerPool);
        taskSubmissionWithoutThrowingSupplier.assertTaskSubmitted();
        executorService.runUntilIdle();
        taskSubmissionWithoutThrowingSupplier.assertTaskRan();
    }

    @Test
    public void exceptionWhenSubmittingTaskToExecutorReleasesWorkerResource() {
        DeterministicScheduler executorService = createDeterministicSchedulerWhichRejectsTheFirstExecution();
        WorkerPool workerPool = createSingleWorkerPool(executorService);

        TestTaskSubmission taskSubmissionWhereExecutorThrows = TestTaskSubmission.submitTask(workerPool);
        taskSubmissionWhereExecutorThrows.assertTaskNotSubmitted();
        executorService.runUntilIdle();
        taskSubmissionWhereExecutorThrows.assertTaskDidNotRun();

        TestTaskSubmission taskSubmissionWhereExecutorDoesNotThrow = TestTaskSubmission.submitTask(workerPool);
        taskSubmissionWhereExecutorDoesNotThrow.assertTaskSubmitted();
        executorService.runUntilIdle();
        taskSubmissionWhereExecutorDoesNotThrow.assertTaskRan();
    }

    @Test
    public void workerPoolRecoversWorkerResourceAfterTaskIsDone() {
        WorkerPool workerPool = createWorkerPool(2);
        try {
            CountDownLatch firstTaskBlocker = new CountDownLatch(1);
            CountDownLatch secondTaskBlocker = new CountDownLatch(1);
            CountDownLatch firstTwoRunningIndicator = new CountDownLatch(2);
            assertThat(workerPool.tryRun(() -> 1, _unused -> {
                        firstTwoRunningIndicator.countDown();
                        await(firstTaskBlocker);
                    }))
                    .isTrue();
            assertThat(workerPool.tryRun(() -> 1, _unused -> {
                        firstTwoRunningIndicator.countDown();
                        await(secondTaskBlocker);
                    }))
                    .isTrue();

            assertThat(timedAwait(firstTwoRunningIndicator)).isTrue();
            assertThat(workerPool.tryRun(() -> 1, _unused -> {})).isFalse();

            firstTaskBlocker.countDown();
            assertThat(timedAwait(firstTaskBlocker)).isTrue();

            // The polling is the best option I came up with, the reason a normal assert
            // does not work here is that the worker release happens after (or as a
            // callback) to the consumer, i.e. it will run *after* the latch is awaited.
            // If this flakes, the maximum polling duration should be increased.
            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                TestTaskSubmission taskSubmission = TestTaskSubmission.submitTask(workerPool);
                taskSubmission.assertTaskSubmitted();
                taskSubmission.assertTaskRan();
            });

            secondTaskBlocker.countDown();
            assertThat(timedAwait(secondTaskBlocker)).isTrue();
        } finally {
            assertThat(workerPool.close()).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 5})
    public void workerPoolCanRunUpToWorkerCountConcurrentTasksAndNotMore(int workerCount) {
        WorkerPool workerPool = createWorkerPool(workerCount);
        try {
            CountDownLatch taskBlocker = new CountDownLatch(1);
            CountDownLatch runningTasksIndicator = new CountDownLatch(workerCount);

            for (int i = 0; i < workerCount; i++) {
                boolean submitted = workerPool.tryRun(() -> 1, _ignored -> {
                    runningTasksIndicator.countDown();
                    await(taskBlocker);
                });
                assertThat(submitted).isTrue();
            }
            assertThat(timedAwait(runningTasksIndicator)).isTrue();

            AtomicBoolean newTaskRan = new AtomicBoolean(false);
            boolean newTaskSubmitted = workerPool.tryRun(() -> 1, _ignored -> newTaskRan.set(true));
            assertThat(newTaskSubmitted).isFalse();

            taskBlocker.countDown();
            assertThat(timedAwait(taskBlocker)).isTrue();
            assertThat(workerPool.close()).isEmpty();
            assertThat(newTaskRan).isFalse();
        } finally {
            assertThat(workerPool.close()).isEmpty();
        }
    }

    private static WorkerPool createSingleWorkerPool(ExecutorService executorService) {
        return new WorkerPoolImpl(new Semaphore(1, true), executorService, SAFE_LOGGABLE_PURPOSE);
    }

    private static WorkerPool createWorkerPool(int workerCount) {
        return WorkerPoolImpl.create(workerCount, SAFE_LOGGABLE_PURPOSE);
    }

    private static DeterministicScheduler createDeterministicSchedulerWhichRejectsTheFirstExecution() {
        DeterministicScheduler executorService = spy(new DeterministicScheduler());
        doThrow(new RejectedExecutionException("test exception"))
                .doCallRealMethod()
                .when(executorService)
                .execute(any(Runnable.class));
        return executorService;
    }

    private static boolean timedAwait(CountDownLatch latch) {
        try {
            return latch.await(3000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static final class TestTaskSubmission {
        private final boolean submitted;
        private final LongAdder supplierInvocationCount;
        private final AtomicBoolean taskRan;
        private final AtomicReference<Integer> resultHolder;

        private TestTaskSubmission(
                boolean submitted,
                AtomicBoolean taskRan,
                LongAdder supplierInvocationCount,
                AtomicReference<Integer> resultHolder) {
            this.submitted = submitted;
            this.supplierInvocationCount = supplierInvocationCount;
            this.taskRan = taskRan;
            this.resultHolder = resultHolder;
        }

        static TestTaskSubmission submitTask(WorkerPool workerPool, int supplierResult) {
            AtomicBoolean taskRan = new AtomicBoolean(false);
            LongAdder supplierInvocationCount = new LongAdder();
            AtomicReference<Integer> resultHolder = new AtomicReference<>();
            boolean submitted = workerPool.tryRun(
                    () -> {
                        supplierInvocationCount.increment();
                        return supplierResult;
                    },
                    result -> {
                        resultHolder.set(result);
                        taskRan.set(true);
                    });
            return new TestTaskSubmission(submitted, taskRan, supplierInvocationCount, resultHolder);
        }

        static TestTaskSubmission submitTask(WorkerPool workerPool) {
            return TestTaskSubmission.submitTask(workerPool, 1);
        }

        static TestTaskSubmission submitTaskWithThrowingSupplier(WorkerPool workerPool) {
            AtomicBoolean taskRan = new AtomicBoolean(false);
            LongAdder supplierInvocationCount = new LongAdder();
            AtomicReference<Integer> resultHolder = new AtomicReference<>();
            boolean submitted = workerPool.<Integer>tryRun(
                    () -> {
                        supplierInvocationCount.increment();
                        throw new RuntimeException("test exception");
                    },
                    result -> {
                        resultHolder.set(result);
                        taskRan.set(true);
                    });
            return new TestTaskSubmission(submitted, taskRan, supplierInvocationCount, resultHolder);
        }

        void assertTaskSubmitted() {
            assertThat(submitted).isTrue();
        }

        void assertTaskNotSubmitted() {
            assertThat(submitted).isFalse();
        }

        void assertSupplierWasConsumedExactlyOnce() {
            assertThat(supplierInvocationCount).hasValue(1);
        }

        void assertSupplierWasNotConsumed() {
            assertThat(supplierInvocationCount).hasValue(0);
        }

        void assertTaskRan() {
            assertThat(taskRan).isTrue();
        }

        void assertTaskDidNotRun() {
            assertThat(taskRan).isFalse();
        }

        void assertConsumedResultWas(int expectedResult) {
            assertThat(resultHolder).hasValue(expectedResult);
        }
    }
}
