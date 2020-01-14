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

import com.palantir.atlasdb.timelock.lock.watch.LockWatchingResource;
import com.palantir.lock.LockService;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;

@Value.Immutable
public interface TimeLockServices {
    static TimeLockServices create(
            TimestampService timestampService,
            LockService lockService,
            AsyncTimelockResource timelockService,
            LockWatchingResource lockWatchingResource,
            TimestampManagementService timestampManagementService) {
        return ImmutableTimeLockServices.builder()
                .timestampService(timestampService)
                .lockService(lockService)
                .timestampManagementService(timestampManagementService)
                .timelockService(timelockService)
                .lockWatchingResource(lockWatchingResource)
                .build();
    }

    TimestampService getTimestampService();
    LockService getLockService();
    AsyncTimelockResource getTimelockService();
    LockWatchingResource getLockWatchingResource();
    TimestampManagementService getTimestampManagementService();
}
