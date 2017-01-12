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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.palantir.atlasdb.timestamp.TimestampServiceTests;
import com.palantir.common.remoting.ServiceNotAvailableException;

public class PersistentTimestampServiceIntegrationTest {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private InMemoryTimestampBoundStore timestampBoundStore = new InMemoryTimestampBoundStore();
    private PersistentTimestampService persistentTimestampService = PersistentTimestampService.create(timestampBoundStore);
    private ExecutorService executor = Executors.newFixedThreadPool(16);

    @Test public void
    timestampsAreReturnedInOrder() {
        TimestampServiceTests.timestampsAreReturnedInOrder(persistentTimestampService);
    }

    @Test public void
    timestampRangesAreReturnedInNonOverlappingOrder() {
        TimestampServiceTests.timestampRangesAreReturnedInNonOverlappingOrder(persistentTimestampService);
    }

    @Test public void
    canRequestMoreTimestampsThanAreAllocatedAtOnce() {
        TimestampServiceTests.canRequestMoreTimestampsThanAreAllocatedAtOnce(persistentTimestampService);
    }

    @Test public void
    willNotHandOutTimestampsEarlierThanAFastForward() {
        TimestampServiceTests.willNotHandOutTimestampsEarlierThanAFastForward(persistentTimestampService, persistentTimestampService);
    }


    @Test public void
    willDoNothingWhenFastForwardToEarlierTimestamp() {
        TimestampServiceTests.willDoNothingWhenFastForwardToEarlierTimestamp(persistentTimestampService, persistentTimestampService);
    }

    @Test public void
    canReturnManyUniqueTimestampsInParallel() throws InterruptedException, TimeoutException {
        TimestampServiceTests.canReturnManyUniqueTimestampsInParallel(persistentTimestampService, executor);
    }

    @Test public void
    shouldLimitRequestsForMoreThanTenThousandTimestamps() {
        assertThat(
                persistentTimestampService.getFreshTimestamps(100 * 1000).size(),
                is(10 * 1000L)
        );
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

    @Test
    public void shouldThrowIfRequestingNegativeNumbersOfTimestamps() {
        TimestampServiceTests.shouldThrowIfRequestingNegativeNumbersOfTimestamps(persistentTimestampService);
    }

    @Test
    public void shouldThrowIfRequestingZeroTimestamps() {
        TimestampServiceTests.shouldThrowIfRequestingZeroTimestamps(persistentTimestampService);
    }

    private void getTimestampAndIgnoreErrors() {
        try {
            persistentTimestampService.getFreshTimestamp();
        } catch (Exception e) {
            // expected
        }
    }
}
