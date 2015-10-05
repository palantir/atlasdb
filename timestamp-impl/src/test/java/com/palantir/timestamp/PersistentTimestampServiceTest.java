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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.junit.Test;

import com.palantir.common.concurrent.PTExecutors;

public class PersistentTimestampServiceTest {
    @Test
    public void testCreation() {
        Mockery m = new Mockery();
        final TimestampBoundStore tbsMock = m.mock(TimestampBoundStore.class);
        final long initialValue = 72;
        m.checking(new Expectations() {{
            oneOf(tbsMock).getUpperLimit();
            returnValue(initialValue);
            oneOf(tbsMock).storeUpperLimit(with(any(Long.class)));
        }});
        PersistentTimestampService.create(tbsMock);
        m.assertIsSatisfied();
    }

    @Test
    public void testLimit() throws InterruptedException {
        Mockery m = new Mockery();
        final TimestampBoundStore tbsMock = m.mock(TimestampBoundStore.class);
        final long initialValue = 0;
        m.checking(new Expectations() {{
            oneOf(tbsMock).getUpperLimit(); returnValue(initialValue);
            oneOf(tbsMock).storeUpperLimit(with(any(Long.class)));
            // Throws exceptions after here, which will prevent allocating more timestamps.
        }});

        // Use up all initially-allocated timestamps.
        final TimestampService tsService = PersistentTimestampService.create(tbsMock);
        for (int i = 1; i <= PersistentTimestampService.ALLOCATION_BUFFER_SIZE; ++i) {
            assertEquals(i, tsService.getFreshTimestamp());
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
