/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.factory.timelock;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;

import com.palantir.lock.v2.AutoDelegate_TimelockService;
import com.palantir.lock.v2.IdentifiedTimeLockRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.StartAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;

/**
 * A timelock service decorator for introducing runtime validity checks on received timestamps.
 */
public class TimestampCorroboratingTimelockServiceTest {
    private TimelockService timelockService;
    private TimelockService corroboratingTimelockService;
    private GoBackInTimeTimestampService goBackInTimeTimestampService;

    @Before
    public void setUp() {
        goBackInTimeTimestampService = new GoBackInTimeTimestampService();
        timelockService = getMockTimelockService(goBackInTimeTimestampService);
        corroboratingTimelockService = TimestampCorroboratingTimelockService.create(timelockService);
    }

    @Test
    public void getFreshTimestampShouldFailIfGoesBackInTime() {
        getIndividualTimestamps(20);
        goBackInTimeFor(10);

        assertThatThrownBy(corroboratingTimelockService::getFreshTimestamp).isInstanceOf(AssertionError.class);
    }

    @Test
    public void getFreshTimestampsShouldFailIfGoesBackInTime() {
        getBatchedTimestamps(20);
        goBackInTimeFor(10);

        assertThatThrownBy(() -> corroboratingTimelockService.getFreshTimestamps(10))
                .isInstanceOf(AssertionError.class);
    }

    private void getIndividualTimestamps(int numberOfTimestamps) {
        for (int i = 0; i < numberOfTimestamps; i++) {
            corroboratingTimelockService.getFreshTimestamp();
        }
    }

    private void getBatchedTimestamps(int numberOfTimestamps) {
        corroboratingTimelockService.getFreshTimestamps(numberOfTimestamps);
    }

    private void goBackInTimeFor(int numberOfTimestamps) {
        goBackInTimeTimestampService.goBackInTime(numberOfTimestamps);
    }

    private TimelockService getMockTimelockService(TimestampService timestampService) {
        TimelockService timelockService = mock(TimelockService.class);
        return new AutoDelegate_TimelockService() {
            @Override
            public TimelockService delegate() {
                return timelockService;
            }

            @Override
            public long getFreshTimestamp() {
                return timestampService.getFreshTimestamp();
            }

            @Override
            public TimestampRange getFreshTimestamps(int numTimestampRequested) {
                return timestampService.getFreshTimestamps(numTimestampRequested);
            }

            @Override
            public StartAtlasDbTransactionResponse startAtlasDbTransaction(IdentifiedTimeLockRequest request) {
                return StartAtlasDbTransactionResponse.of(mock(LockImmutableTimestampResponse.class),
                        timestampService.getFreshTimestamp());
            }
        };
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