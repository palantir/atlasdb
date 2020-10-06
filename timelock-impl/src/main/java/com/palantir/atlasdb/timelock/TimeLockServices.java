/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.timelock;

import org.immutables.value.Value;

import com.google.common.util.concurrent.Futures;
import com.palantir.lock.LockService;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;

@Value.Immutable
public interface TimeLockServices {
    static TimeLockServices create(
            LockService lockService,
            AsyncTimelockService timelockService,
            AsyncTimelockResource timelockResource) {
        return ImmutableTimeLockServices.builder()
                .lockService(lockService)
                .timelockService(timelockService)
                .timelockResource(timelockResource)
                .build();
    }

    // The Jersey endpoints
    AsyncTimelockResource getTimelockResource();
    // The RPC-independent leadership-enabled implementation of the timelock service
    AsyncTimelockService getTimelockService();

    @Deprecated
    LockService getLockService();

    @Deprecated
    @Value.Derived
    default TimestampService getTimestampService() {
        return new TimestampService() {
            @Override
            public long getFreshTimestamp() {
                return Futures.getUnchecked(getTimelockService().getFreshTimestamp());
            }

            @Override
            public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
                return Futures.getUnchecked(getTimelockService().getFreshTimestamps(numTimestampsRequested));
            }
        };
    }

    @Deprecated
    @Value.Derived
    default TimestampManagementService getTimestampManagementService() {
        return new TimestampManagementService() {
            @Override
            public void fastForwardTimestamp(long currentTimestamp) {
                Futures.getUnchecked(getTimelockService().fastForwardTimestamp(currentTimestamp));
            }

            @Override
            public String ping() {
                return Futures.getUnchecked(getTimelockService().ping());
            }
        };
    }
}
