/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.timestamp;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.IntStream.range;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.palantir.common.remoting.ServiceNotAvailableException;

public class PersistentTimestampServiceIntegrationTest {
    private static final long ONE_MILLION = 1000 * 1000;
    private static final long TWO_MILLION = 2 * ONE_MILLION;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private InMemoryTimestampBoundStore timestampBoundStore = new InMemoryTimestampBoundStore();
    private PersistentTimestampService persistentTimestampService = PersistentTimestampService.create(timestampBoundStore);
    private ExecutorService executor = Executors.newFixedThreadPool(16);

    @Test public void
    tiemstampsAreReturnedInOrder() {
        List<Long> timestamps = new ArrayList<>();

        timestamps.add(persistentTimestampService.getFreshTimestamp());
        timestamps.add(persistentTimestampService.getFreshTimestamp());
        timestamps.add(persistentTimestampService.getFreshTimestamp());

        assertThat(timestamps, contains(1L, 2L, 3L));
    }

    @Test public void
    timestampRangesAreReturnedInNonOverlappingOrder() {
        List<TimestampRange> timestampRanges = new ArrayList<>();

        timestampRanges.add(persistentTimestampService.getFreshTimestamps(10));
        timestampRanges.add(persistentTimestampService.getFreshTimestamps(10));

        long firstUpperBound = timestampRanges.get(0).getUpperBound();
        long secondLowerBound = timestampRanges.get(1).getLowerBound();

        assertThat(firstUpperBound, is(lessThan(secondLowerBound)));
    }


    @Test public void
    canRequestMoreTimestampsThanAreAllocatedAtOnce() {
        for(int i = 0; i < ONE_MILLION / 1000; i++) {
            persistentTimestampService.getFreshTimestamps(1000);
        }

        assertThat(persistentTimestampService.getFreshTimestamp(), is(ONE_MILLION + 1));
    }

    @Test public void
    shouldLimitRequestsForMoreThanTenThousandTimestamps() {
        assertThat(
                persistentTimestampService.getFreshTimestamps(100 * 1000).size(),
                is(10 * 1000L)
        );
    }

    @Test public void
    willNotHandOutTimestampsEarlierThanAFastForward() {
        persistentTimestampService.fastForwardTimestamp(TWO_MILLION);

        assertThat(
                persistentTimestampService.getFreshTimestamp(),
                is(greaterThan(TWO_MILLION)));
    }

    @Test public void
    canReturnManyUniqueTimestampsInParallel() throws InterruptedException, TimeoutException {
        Set<Long> uniqueTimestamps = new ConcurrentSkipListSet<>();

        repeat(TWO_MILLION, new Runnable() {
            @Override
            public void run() {
                uniqueTimestamps.add(persistentTimestampService.getFreshTimestamp());
            }
        });

        assertThat(uniqueTimestamps.size(), is((int) TWO_MILLION));
    }

    @Test public void
    attemptToReproduceMultipleTimestampsError() throws TimeoutException, InterruptedException, ExecutionException {
        // Create PersistentTimestampService to give us access to AvailableTimestamps.ALLOCATION_BUFFER_SIZE
        PersistentUpperLimit upperLimit = new PersistentUpperLimit(timestampBoundStore);
        LastReturnedTimestamp lastReturned = new LastReturnedTimestamp(upperLimit.get());
        AvailableTimestamps availableTimestamps = new AvailableTimestamps(lastReturned, upperLimit, 10); // really tiny allocation buffer size

        ExecutorService manyThreads = Executors.newFixedThreadPool(10);
        PersistentTimestampService testTimestampService = new PersistentTimestampService(availableTimestamps, manyThreads);

        CyclicBarrier barrier = new CyclicBarrier(48);


        AtomicInteger numThreads = new AtomicInteger();
        IntConsumer intConsumer = value -> {
            try {
                System.out.println("Hi! " + numThreads.incrementAndGet());
                barrier.await();
                System.out.println("My turn! " + value);
                long freshTimestamp = testTimestampService.getFreshTimestamp();
                System.out.println("I, " + value + ", got: (" +
                        freshTimestamp);
                System.out.println(availableTimestamps.getUpperLimit());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        ForkJoinPool pool = new ForkJoinPool(48);
        ForkJoinTask<?> task = pool.submit(() -> range(100, 148).parallel().
                forEach(intConsumer)
        );

        task.get(10, TimeUnit.SECONDS);
    }

    @Test public void
    refreshAndHandOutAtTheSameTime() {
        // Create PersistentTimestampService to give us access to AvailableTimestamps.ALLOCATION_BUFFER_SIZE
        PersistentUpperLimit upperLimit = new PersistentUpperLimit(timestampBoundStore);
        LastReturnedTimestamp lastReturned = new LastReturnedTimestamp(upperLimit.get());
        AvailableTimestamps availableTimestamps = new AvailableTimestamps(lastReturned, upperLimit, 10); // really tiny allocation buffer size

        availableTimestamps.handOut(5);
        availableTimestamps.refreshBuffer();
    }

    @Test(expected = ServiceNotAvailableException.class) public void
    throwsAserviceNotAvailableExceptionIfThereAreMultipleServersRunning() {
        timestampBoundStore.pretendMultipleServersAreRunning();

        persistentTimestampService.getFreshTimestamp();
    }

    @Test public void
    shouldRethrowAllocationExceptions() {
        final IllegalArgumentException failure = new IllegalArgumentException();
        exception.expect(RuntimeException.class);
        exception.expectCause(is(failure));

        timestampBoundStore.failWith(failure);

        persistentTimestampService.getFreshTimestamp();
    }

    @Test public void
    shouldNotTryToStoreANewBoundIfMultipleServicesAreRunning() {
        timestampBoundStore.pretendMultipleServersAreRunning();

        getTimestampAndIgnoreErrors();
        getTimestampAndIgnoreErrors();

        assertThat(timestampBoundStore.numberOfAllocations(), is(lessThan(2)));
    }

    private void repeat(long count, Runnable task) throws InterruptedException, TimeoutException {
        for(int i = 0; i < count; i++) {
            executor.submit(task);
        }

        executor.shutdown();
        executor.awaitTermination(1, MINUTES);

        if(!executor.isTerminated()) {
            throw new TimeoutException("Timed out waiting for the executor to terminate");
        }
    }

    private void getTimestampAndIgnoreErrors() {
        try {
            persistentTimestampService.getFreshTimestamp();
        } catch (Exception e) {
            // expected
        }
    }
}
