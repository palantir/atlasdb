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

package com.palantir.atlasdb.cassandra;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.keyvalue.cassandra.ReloadingCloseableContainer;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.refreshable.Refreshable;
import com.palantir.refreshable.SettableRefreshable;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ReloadingCloseableContainerTest {
    private static final ExecutorService executor =
            PTExecutors.newCachedThreadPool(ReloadingCloseableContainerTest.class.getName());

    private static final int INITIAL_VALUE = 0;
    private static final int UPDATED_VALUE = 1;

    @Mock
    private Closeable mockCloseable;

    @Mock
    private Closeable mockCloseableExtra;

    @Mock
    private Function<Integer, Closeable> closeableFactory;

    private SettableRefreshable<Integer> settableRefreshable;

    @Before
    public void setUp() {
        settableRefreshable = Refreshable.create(INITIAL_VALUE);
        when(closeableFactory.apply(INITIAL_VALUE)).thenReturn(mockCloseable);
        when(closeableFactory.apply(UPDATED_VALUE)).thenReturn(mockCloseableExtra);
    }

    @Test
    public void closeBlocksDuringRunWithInvocation() throws BrokenBarrierException, InterruptedException {
        ReloadingCloseableContainer<Closeable> container = getContainer();
        CyclicBarrier runWithFutureRunsFirstBarrier = new CyclicBarrier(2);
        CyclicBarrier manuallyFinishRunWith = new CyclicBarrier(2);

        executor.submit(() -> container.executeWithResource(closeable -> {
            awaitOrFail(runWithFutureRunsFirstBarrier);
            awaitOrFail(manuallyFinishRunWith);
            verifyNoInteractions(closeable);
        }));

        // This test could pass spuriously, if close is just not called.
        // The likelihood if this is minimised by
        // 1. waiting on a barrier for the executeWithResource and,
        // 2. Requiring a whole second of close not completing.
        Future<?> closeFuture = executor.submit(() -> {
            awaitOrFail(runWithFutureRunsFirstBarrier);
            container.close();
        });
        Awaitility.await().during(Duration.ofSeconds(1)).until(() -> !closeFuture.isDone());
        verifyNoInteractions(mockCloseable);

        manuallyFinishRunWith.await();
        Awaitility.await().atMost(Duration.ofSeconds(1)).until(closeFuture::isDone);
    }

    @Test
    public void refreshCloseBlockedDuringRunWithInvocation()
            throws IOException, BrokenBarrierException, InterruptedException {
        ReloadingCloseableContainer<Closeable> container = getContainer();

        CyclicBarrier runWithFutureRunsFirstBarrier = new CyclicBarrier(2);
        CyclicBarrier manuallyFinishRunWith = new CyclicBarrier(2);

        executor.submit(() -> container.executeWithResource(closeable -> {
            awaitOrFail(runWithFutureRunsFirstBarrier);
            awaitOrFail(manuallyFinishRunWith);
            verifyNoInteractions(closeable);
        }));

        Future<?> refreshableFuture = executor.submit(() -> {
            awaitOrFail(runWithFutureRunsFirstBarrier);
            settableRefreshable.update(UPDATED_VALUE);
        });

        Awaitility.await().during(Duration.ofSeconds(1)).until(() -> !refreshableFuture.isDone());
        verifyNoInteractions(mockCloseable);

        manuallyFinishRunWith.await();

        Awaitility.await().atMost(Duration.ofSeconds(1)).until(refreshableFuture::isDone);
        verify(mockCloseable).close();
    }

    @Test
    public void refreshUpdatesResource() {
        ReloadingCloseableContainer<Closeable> container = getContainer();

        container.executeWithResource(closeable -> assertThat(closeable).isEqualTo(mockCloseable));

        settableRefreshable.update(UPDATED_VALUE);
        container.executeWithResource(closeable -> assertThat(closeable).isEqualTo(mockCloseableExtra));
    }

    @Test
    public void previousResourceClosedAfterRefresh() throws IOException {
        // Creating a container has the side effect of managing all resources created within it.
        getContainer();

        verifyNoInteractions(mockCloseable);
        settableRefreshable.update(UPDATED_VALUE);
        verify(mockCloseable).close();
    }

    @Test
    public void latestResourceClosedAfterClose() throws IOException {
        ReloadingCloseableContainer<Closeable> container = getContainer();

        verifyNoInteractions(mockCloseable);
        settableRefreshable.update(UPDATED_VALUE);
        container.close();

        verify(mockCloseableExtra).close();
    }

    @Test
    public void noFurtherResourceCreatedAfterClose() {
        ReloadingCloseableContainer<Closeable> container = getContainer();

        verifyNoInteractions(mockCloseable);
        container.close();
        settableRefreshable.update(UPDATED_VALUE);

        verify(closeableFactory, times(1)).apply(any());
    }

    @Test
    public void runWithResourceFailsAfterClose() {
        ReloadingCloseableContainer<Closeable> container = getContainer();

        container.close();

        assertThatThrownBy(() -> container.runWithResource(_closeable -> null))
                .isInstanceOf(SafeIllegalStateException.class);
    }

    @Test
    public void closeReleasesLocksAfterException() throws IOException {
        doThrow(new SafeIllegalStateException()).when(mockCloseable).close();

        ReloadingCloseableContainer<Closeable> container = getContainer();
        assertThatThrownBy(container::close).isInstanceOf(SafeIllegalStateException.class);

        Future<?> future = executor.submit(container::close);
        Awaitility.await().atMost(Duration.ofSeconds(1)).until(future::isDone);
    }

    @Test
    public void runWithResourceReleasesLocksAfterExceptionInTask() {
        ReloadingCloseableContainer<Closeable> container = getContainer();

        assertThatThrownBy(() -> container.executeWithResource(_closeable -> {
                    throw new SafeIllegalStateException("Test");
                }))
                .isInstanceOf(SafeIllegalStateException.class);

        Future<?> closeFuture = executor.submit(container::close);
        Awaitility.await().atMost(Duration.ofSeconds(1)).until(closeFuture::isDone);
    }

    @Test
    public void concurrentRunWithWithoutBlocking() {
        int numberOfRuns = 100;
        ReloadingCloseableContainer<Closeable> container = getContainer();
        CyclicBarrier allEnteredRunWith = new CyclicBarrier(numberOfRuns);

        CompletableFuture<?> allFutures = runManyTimesAndGetSingleFuture(
                numberOfRuns, () -> container.executeWithResource(_closeable -> awaitOrFail(allEnteredRunWith)));

        Awaitility.await().atMost(Duration.ofSeconds(2)).until(allFutures::isDone);
    }

    @Test
    public void closeSuppressesUnderlyingIoException() throws IOException {
        doThrow(new IOException()).when(mockCloseable).close();
        ReloadingCloseableContainer<Closeable> container = getContainer();
        container.close();
    }

    @Test
    public void blockingMethodsDontDeadlockUnderConcurrentLoad() {
        int numberOfRuns = 2000;

        ReloadingCloseableContainer<Closeable> container = getContainer();

        AtomicInteger refreshableValue = new AtomicInteger(INITIAL_VALUE);
        CompletableFuture<?> allFutures =
                runManyTimesAndGetSingleFuture(numberOfRuns, () -> triggerEvent(container, refreshableValue));

        Awaitility.await().atMost(Duration.ofSeconds(2)).until(allFutures::isDone);

        Future<?> future = executor.submit(container::close);
        Awaitility.await().atMost(Duration.ofSeconds(1)).until(future::isDone);
    }

    @Test
    public void runWithResourceReturnsFunctionValue() {
        ReloadingCloseableContainer<Closeable> container = getContainer();
        int expectedValue = 3213;
        int returnedValue = container.runWithResource(_closeable -> expectedValue);
        assertThat(returnedValue).isEqualTo(expectedValue);
    }

    @Test
    public void runWithResourceRunsWithLatestResource() {
        ReloadingCloseableContainer<Closeable> container =
                ReloadingCloseableContainer.of(closeableFactory, settableRefreshable);
        settableRefreshable.update(UPDATED_VALUE);
        container.executeWithResource(closeable -> assertThat(closeable).isEqualTo(mockCloseableExtra));
    }

    @Test
    public void runWithResourcePropagatesException() {
        ReloadingCloseableContainer<Closeable> container = getContainer();
        assertThatThrownBy(() -> container.runWithResource(_closeable -> {
                    throw new SafeIllegalStateException("Test");
                }))
                .isInstanceOf(SafeIllegalStateException.class);
    }

    @Test
    public void runWithDoesNotBlockResourceUpdates() {
        ReloadingCloseableContainer<Closeable> container = getContainer();
        CyclicBarrier ensureInitialExecuteHasStartedBarrier = new CyclicBarrier(2);
        CyclicBarrier manuallyFinishRunWithBarrier = new CyclicBarrier(2);
        executor.submit(() -> container.executeWithResource(closeable -> {
            awaitOrFail(ensureInitialExecuteHasStartedBarrier);
            awaitOrFail(manuallyFinishRunWithBarrier);
            assertThat(closeable).isEqualTo(mockCloseable);
        }));

        awaitOrFail(ensureInitialExecuteHasStartedBarrier);
        // Map and subscriber updates happen sequentially, but on the same thread. Therefore, the next call won't
        // complete until after the execute above completes, even if the map itself has finished.
        executor.submit(() -> settableRefreshable.update(UPDATED_VALUE));

        // The earlier executeWithResource call cannot complete yet, and so if the update was blocked, this following
        // assertion would fail.

        // It is possible for this to fail, if this call ends up scheduled before the update has started...
        Future<?> executeWithResourceFuture = executor.submit(() ->
                container.executeWithResource(closeable -> assertThat(closeable).isEqualTo(mockCloseableExtra)));

        Awaitility.await().atMost(Duration.ofSeconds(1)).until(executeWithResourceFuture::isDone);
        awaitOrFail(manuallyFinishRunWithBarrier);
    }

    private void triggerEvent(ReloadingCloseableContainer<Closeable> container, AtomicInteger refreshableValue) {
        double random = ThreadLocalRandom.current().nextDouble();
        if (random < 0.2) {
            settableRefreshable.update(refreshableValue.incrementAndGet());
        } else {
            try {
                container.executeWithResource(_closeable -> {});
            } catch (Throwable t) {
                fail("Triggering an event caused an exception");
            }
        }
    }

    private CompletableFuture<?> runManyTimesAndGetSingleFuture(int numberOfRuns, Runnable task) {
        CompletableFuture<?>[] allFutures = IntStream.range(0, numberOfRuns)
                .mapToObj(_id -> CompletableFuture.runAsync(task, executor))
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.anyOf(allFutures);
    }

    private ReloadingCloseableContainer<Closeable> getContainer() {
        return ReloadingCloseableContainer.of(closeableFactory, settableRefreshable);
    }

    private static void awaitOrFail(CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            fail("Failed to await barrier");
        }
    }
}
