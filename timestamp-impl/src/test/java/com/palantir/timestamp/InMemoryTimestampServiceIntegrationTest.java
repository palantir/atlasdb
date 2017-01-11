/**
 * Copyright 2017 Palantir Technologies
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

import java.util.concurrent.TimeoutException;

import org.junit.Test;

public class InMemoryTimestampServiceIntegrationTest {

    private InMemoryTimestampService inMemoryTimestampService = new InMemoryTimestampService();

    @Test public void
    timestampsAreReturnedInOrder() {
        TimestampServiceTests.timestampsAreReturnedInOrder(inMemoryTimestampService);
    }

    @Test public void
    timestampRangesAreReturnedInNonOverlappingOrder() {
        TimestampServiceTests.timestampRangesAreReturnedInNonOverlappingOrder(inMemoryTimestampService);
    }

    @Test public void
    canRequestMoreTimestampsThanAreAllocatedAtOnce() {
        TimestampServiceTests.canRequestMoreTimestampsThanAreAllocatedAtOnce(inMemoryTimestampService);
    }

    @Test public void
    willNotHandOutTimestampsEarlierThanAFastForward() {
        TimestampServiceTests.willNotHandOutTimestampsEarlierThanAFastForward(inMemoryTimestampService, inMemoryTimestampService);
    }

    @Test public void
    willDoNothingWhenFastForwardToEarlierTimestamp() {
        TimestampServiceTests.willDoNothingWhenFastForwardToEarlierTimestamp(inMemoryTimestampService, inMemoryTimestampService);
    }

    @Test public void
    canReturnManyUniqueTimestampsInParallel() throws InterruptedException, TimeoutException {
        TimestampServiceTests.canReturnManyUniqueTimestampsInParallel(inMemoryTimestampService);
    }

    @Test public void
    shouldThrowIfRequestingNegativeNumbersOfTimestamps() {
        TimestampServiceTests.shouldThrowIfRequestingNegativeNumbersOfTimestamps(inMemoryTimestampService);
    }

    @Test public void
    shouldThrowIfRequestingZeroTimestamps() {
        TimestampServiceTests.shouldThrowIfRequestingZeroTimestamps(inMemoryTimestampService);
    }
}
