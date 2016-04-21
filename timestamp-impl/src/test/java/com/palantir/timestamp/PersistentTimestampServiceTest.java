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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.junit.Test;

import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.remoting.ServiceNotAvailableException;

public class PersistentTimestampServiceTest {
    @Test
    public void testFastForward() {
        Mockery m = new Mockery();
        final TimestampBoundStore tbsMock = m.mock(TimestampBoundStore.class);
        final long initialValue = 1_234_567L;
        final long futureTimestamp = 12_345_678L;
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

    @Test
    public void notThrowOnCreateIfBoundStoreIsInvalid() {
        PersistentTimestampService.create(failingTimestampBoundStore());
    }

    @Test(expected = ServiceNotAvailableException.class)
    public void throwOnTimestampRequestIfBoundStoreIsInvalid() {
        PersistentTimestampService persistentTimestampService = PersistentTimestampService.create(failingTimestampBoundStore());

        persistentTimestampService.getFreshTimestamp();
    }

    private TimestampBoundStore failingTimestampBoundStore() {
        TimestampBoundStore timestampBoundStore = mock(TimestampBoundStore.class);
        when(timestampBoundStore.getUpperLimit()).thenReturn(0L);
        doThrow(new MultipleRunningTimestampServiceError("error")).when(timestampBoundStore).storeUpperLimit(anyLong());
        return timestampBoundStore;
    }

    @Test
    public void testLimit() throws InterruptedException {
        Mockery m = new Mockery();
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
            f.get(10, TimeUnit.MILLISECONDS);
            fail("We should be blocking");
        } catch (ExecutionException e) {
            // we expect this failure because we can't allocate timestamps
        } catch (TimeoutException e) {
            // We expect this timeout, as we're blocking.
        } finally {
            f.cancel(true);
            exec.shutdown();
        }

        m.assertIsSatisfied();
    }
}
