/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

public class CorroboratingTimestampServiceTests {

    @Test
    public void rawServiceDoesNotFailIfGoesBackInTime() {
        GoBackInTimeTimestampService rawService = new GoBackInTimeTimestampService();
        TimestampService timestampService = new RequestBatchingTimestampService(rawService);

        for (int i = 0; i < 20; i++) {
            timestampService.getFreshTimestamp();
        }

        rawService.goBackInTime(10);
        assertEquals(11L, timestampService.getFreshTimestamp());
    }

    @Test
    public void shouldFailIfGoesBackInTime() {
        GoBackInTimeTimestampService rawService = new GoBackInTimeTimestampService();
        TimestampService timestampService = new RequestBatchingTimestampService(
                new CorroboratingTimelockService(rawService));

        for (int i = 0; i < 20; i++) {
            timestampService.getFreshTimestamp();
        }

        rawService.goBackInTime(10);
        assertEquals(11L, timestampService.getFreshTimestamp());
    }

    static class GoBackInTimeTimestampService implements TimestampService, TimestampManagementService {
        private final AtomicLong counter = new AtomicLong(0);

        public void goBackInTime(long time) {
            counter.getAndUpdate(x -> {
                if (x < time) {
                    return 0;
                }
                return x - time;
            });
        }

        @Override
        public long getFreshTimestamp() {
            return counter.incrementAndGet();
        }

        @Override
        public TimestampRange getFreshTimestamps(int timestampsToGet) {
            if (timestampsToGet <= 0) {
                throw new IllegalArgumentException("Argument must be positive: " + timestampsToGet);
            }
            long topValue = counter.addAndGet(timestampsToGet);
            return TimestampRange.createInclusiveRange(topValue - timestampsToGet + 1, topValue);
        }

        @Override
        public void fastForwardTimestamp(long currentTimestamp) {
            long latestTimestampFromService = counter.get();
            while (latestTimestampFromService < currentTimestamp) {
                counter.compareAndSet(latestTimestampFromService, currentTimestamp);
                latestTimestampFromService = counter.get();
            }
        }

        @Override
        public String ping() {
            return PING_RESPONSE;
        }
    }
}
