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

import com.palantir.leader.Renewable;
import com.palantir.lock.LockService;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;
import org.immutables.value.Value;

@Value.Immutable
public interface TimeLockServices extends Renewable {
    static TimeLockServices create(
            TimestampService timestampService,
            LockService lockService,
            AsyncTimelockService timelockService,
            AsyncTimelockResource timelockResource,
            TimestampManagementService timestampManagementService,
            Runnable renewalCallback) {
        return ImmutableTimeLockServices.builder()
                .timestampService(timestampService)
                .lockService(lockService)
                .timestampManagementService(timestampManagementService)
                .timelockService(timelockService)
                .timelockResource(timelockResource)
                .renewalCallback(renewalCallback)
                .build();
    }

    TimestampService getTimestampService();

    LockService getLockService();
    // The Jersey endpoints
    AsyncTimelockResource getTimelockResource();
    // The RPC-independent leadership-enabled implementation of the timelock service
    AsyncTimelockService getTimelockService();

    TimestampManagementService getTimestampManagementService();

    Runnable renewalCallback();

    @Override
    default void renew() {
        renewalCallback().run();
    }
}
