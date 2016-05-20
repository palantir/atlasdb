/**
 * Copyright 2015 Palantir Technologies
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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.longThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.common.time.Clock;

public class PersistentTimestampServiceIntegrationTest {

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private final TimestampBoundStore timestampBoundStore = mock(TimestampBoundStore.class);
    private final Clock clock = mock(Clock.class);
    private PersistentUpperLimit upperLimit;

    @Before
    public void setup() {
        when(timestampBoundStore.getUpperLimit()).thenReturn(0L);
        upperLimit = new PersistentUpperLimit(timestampBoundStore, clock);
    }


    @Test
    public void testFastForward() {
        Mockery m = new Mockery();
        m.setThreadingPolicy(new Synchroniser());
        final TimestampBoundStore tbsMock = m.mock(TimestampBoundStore.class);
        final long initialValue = 1234567L;
        final long futureTimestamp = 12345678L;
        m.checking(new Expectations() {{
            oneOf(tbsMock).getUpperLimit(); will(returnValue(initialValue));
            oneOf(tbsMock).storeUpperLimit(initialValue + PersistentTimestampService.ALLOCATION_BUFFER_SIZE);
            oneOf(tbsMock).storeUpperLimit(futureTimestamp + PersistentTimestampService.ALLOCATION_BUFFER_SIZE);
        }});

        final PersistentTimestampService ptsService = PersistentTimestampService.create(tbsMock);
        for (int i = 1; i <= 1000; i++) {
            assertEquals(initialValue+i, ptsService.getFreshTimestamp());
        }

        ptsService.fastForwardTimestamp(futureTimestamp);
        for (int i = 1; i <= 1000; i++) {
            assertEquals(futureTimestamp+i, ptsService.getFreshTimestamp());
        }

        m.assertIsSatisfied();
    }

    @Test(expected = ServiceNotAvailableException.class)
    public void shouldThrowAServiceNotAvailableExceptionIfMultipleTimestampSerivcesAreRunning() {
        final TimestampBoundStore timestampBoundStore = timestampStoreFailingWith(new MultipleRunningTimestampServiceError("error"));

        PersistentTimestampService persistentTimestampService = PersistentTimestampService.create(timestampBoundStore);
        persistentTimestampService.getFreshTimestamp();
    }

    @Test
    public void incrementUpperLimitIfOneMinuteElapsedSinceLastUpdate() throws InterruptedException {
        givenTheTimeIs(0, MINUTES);
        PersistentTimestampService persistentTimestampService = new PersistentTimestampService(upperLimit, new LastReturnedTimestamp(upperLimit.get()));

        givenTheTimeIs(30, SECONDS);
        persistentTimestampService.getFreshTimestamp();

        verify(timestampBoundStore).storeUpperLimit(PersistentTimestampService.ALLOCATION_BUFFER_SIZE);

        givenTheTimeIs(3, MINUTES);
        persistentTimestampService.getFreshTimestamp();

        verify(timestampBoundStore).storeUpperLimit(
                longThat(is(greaterThan(PersistentTimestampService.ALLOCATION_BUFFER_SIZE))));
    }

    private void givenTheTimeIs(int time, TimeUnit unit) {
        when(clock.getTimeMillis()).thenReturn(unit.toMillis(time));
    }

    @Test
    public void incrementUpperLimitOnFirstFreshTimestampRequest() {
        PersistentTimestampService persistentTimestampService = PersistentTimestampService.create(timestampBoundStore);

        persistentTimestampService.getFreshTimestamp();

        verify(timestampBoundStore).storeUpperLimit(PersistentTimestampService.ALLOCATION_BUFFER_SIZE);
    }

    @Test
    public void multipleFreshTimestampRequestsShouldIncreaseUpperLimitOnlyOnce() {
        PersistentTimestampService persistentTimestampService = PersistentTimestampService.create(timestampBoundStore);

        getFreshTimestampsInParallel(persistentTimestampService, 20);

        verify(timestampBoundStore, times(1)).storeUpperLimit(PersistentTimestampService.ALLOCATION_BUFFER_SIZE);
    }

    @Test
    public void throwOnTimestampRequestIfBoundStoreCannotStoreNewUpperLimit() {
        PersistentTimestampService persistentTimestampService = PersistentTimestampService.create(timestampStoreFailingWith(new RuntimeException()));

        expectedException.expect(RuntimeException.class);
        persistentTimestampService.getFreshTimestamp();
    }

    @Test
    public void testLimit() throws InterruptedException {
        Mockery m = new Mockery();
        m.setThreadingPolicy(new Synchroniser());
        final TimestampBoundStore tbsMock = m.mock(TimestampBoundStore.class);
        final long initialValue = 72;
        m.checking(new Expectations() {{
            oneOf(tbsMock).getUpperLimit(); will(returnValue(initialValue));
            oneOf(tbsMock).storeUpperLimit(with(any(Long.class)));
            // Throws exceptions after here, which will prevent allocating more timestamps.
        }});

        // Use up all initially-allocated timestamps.
        final TimestampService tsService = PersistentTimestampService.create(tbsMock);
        for (int i = 1; i <= PersistentTimestampService.ALLOCATION_BUFFER_SIZE; ++i) {
            assertEquals(initialValue+i, tsService.getFreshTimestamp());
        }

        ExecutorService exec = PTExecutors.newSingleThreadExecutor();
        Future<?> f = exec.submit(new Runnable() {
            @Override
            public void run() {
                // This will block.
                tsService.getFreshTimestamp();
            }
        });

        try {
            f.get(10, MILLISECONDS);
            fail("We should be blocking");
        } catch (ExecutionException e) {
            // we expect this failure because we can't allocate timestamps
        } catch (TimeoutException e) {
            // We expect this timeout, as we're blocking.
        } finally {
            f.cancel(true);
            exec.shutdown();
        }
    }

    private void getFreshTimestampsInParallel(PersistentTimestampService persistentTimestampService, int numTimes) {
        ExecutorService executorService = Executors.newFixedThreadPool(numTimes / 2);
        try {
            List<Future<Long>> futures = Lists.newArrayListWithExpectedSize(numTimes);
            for (int i = 0; i < numTimes; i++) {
                Future<Long> future = executorService.submit(new Callable<Long>() {
                    @Override
                    public Long call() throws Exception {
                        return persistentTimestampService.getFreshTimestamp();
                    }
                });
                futures.add(future);
            }
            for (int i = 0; i < futures.size(); i++) {
                Futures.getUnchecked(futures.get(i));
            }
        } finally {
            executorService.shutdown();
        }
    }

    private TimestampBoundStore timestampStoreFailingWith(Throwable throwable) {
        TimestampBoundStore timestampBoundStore = mock(TimestampBoundStore.class);
        when(timestampBoundStore.getUpperLimit()).thenReturn(0L);
        doThrow(throwable).when(timestampBoundStore).storeUpperLimit(anyLong());
        return timestampBoundStore;
    }
}
