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
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;

public class TimestampCorroboratingTimelockServiceTest {
    private static final IdentifiedTimeLockRequest IDENTIFIED_TIME_LOCK_REQUEST = IdentifiedTimeLockRequest.create();

    private TimelockService timelockService;
    private GoBackInTimeTimestampService goBackInTimeTimestampService;

    @Before
    public void setUp() {
        goBackInTimeTimestampService = new GoBackInTimeTimestampService();
        timelockService = TimestampCorroboratingTimelockService.create(
                getMockTimelockService(goBackInTimeTimestampService));
    }

    @Test
    public void getFreshTimestampShouldFail() {
        getIndividualTimestamps(20);
        goBackInTimeFor(10);

        assertThatThrownBy(timelockService::getFreshTimestamp)
                .isInstanceOf(AssertionError.class)
                .hasMessage(generateClocksWentBackwardsMessage(20, 11));
    }

    @Test
    public void getFreshTimestampsShouldFail() {
        getBatchedTimestamps(20);
        goBackInTimeFor(10);

        assertThatThrownBy(() -> timelockService.getFreshTimestamps(10))
                .isInstanceOf(AssertionError.class)
                .hasMessage(generateClocksWentBackwardsMessage(20, 11));
    }

    @Test
    public void startAtlasDbTransactionShouldFail() {
        timelockService.startAtlasDbTransaction(IDENTIFIED_TIME_LOCK_REQUEST);
        goBackInTimeFor(1);

        assertThatThrownBy(() -> timelockService.startAtlasDbTransaction(IDENTIFIED_TIME_LOCK_REQUEST))
                .isInstanceOf(AssertionError.class)
                .hasMessage(generateClocksWentBackwardsMessage(1, 1));
    }

    @Test
    public void lockImmutableTimestampShouldFail() {
        timelockService.lockImmutableTimestamp(IDENTIFIED_TIME_LOCK_REQUEST);
        goBackInTimeFor(1);

        assertThatThrownBy(() -> timelockService.lockImmutableTimestamp(IDENTIFIED_TIME_LOCK_REQUEST))
                .isInstanceOf(AssertionError.class)
                .hasMessage(generateClocksWentBackwardsMessage(1, 1));
    }

    private static String generateClocksWentBackwardsMessage(long lowerBound, long freshTimestamp) {
        return String.format(TimestampCorroboratingTimelockService.CLOCKS_WENT_BACKWARDS_MESSAGE,
                lowerBound, freshTimestamp);
    }

    private void getIndividualTimestamps(int numberOfTimestamps) {
        for (int i = 0; i < numberOfTimestamps; i++) {
            timelockService.getFreshTimestamp();
        }
    }

    private void getBatchedTimestamps(int numberOfTimestamps) {
        timelockService.getFreshTimestamps(numberOfTimestamps);
    }

    private void goBackInTimeFor(int numberOfTimestamps) {
        goBackInTimeTimestampService.goBackInTime(numberOfTimestamps);
    }

    private TimelockService getMockTimelockService(TimestampService timestampService) {
        TimelockService mockTimelockService = mock(TimelockService.class);
        return new AutoDelegate_TimelockService() {
            @Override
            public TimelockService delegate() {
                return mockTimelockService;
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

            @Override
            public LockImmutableTimestampResponse lockImmutableTimestamp(IdentifiedTimeLockRequest request) {
                return LockImmutableTimestampResponse.of(timestampService.getFreshTimestamp(),
                        mock(LockToken.class));
            }
        };
    }

    static class GoBackInTimeTimestampService implements TimestampService, TimestampManagementService {
        private final AtomicLong counter = new AtomicLong(0);

        public void goBackInTime(long time) {
            counter.getAndUpdate(x -> Math.max(0, x - time));
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
