/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.timelock;

import org.immutables.value.Value;

import com.palantir.atlasdb.timelock.util.AsyncOrLegacyTimelockService;
import com.palantir.lock.LockService;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;

@Value.Immutable
public interface TimeLockServices {
    static TimeLockServices create(
            TimestampService timestampService,
            LockService lockService,
            AsyncOrLegacyTimelockService timelockService,
            TimestampManagementService timestampManagementService) {
        return ImmutableTimeLockServices.builder()
                .timestampService(timestampService)
                .lockService(lockService)
                .timestampManagementService(timestampManagementService)
                .timelockService(timelockService)
                .build();
    }

    TimestampManagementService getTimestampManagementService();
    TimestampService getTimestampService();
    AsyncOrLegacyTimelockService getTimelockService();
    LockService getLockService();
}
