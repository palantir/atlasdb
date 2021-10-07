/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.factory;

import com.palantir.lock.LockClient;
import com.palantir.lock.LockService;
import com.palantir.lock.impl.LegacyTimelockService;
import com.palantir.timelock.paxos.InMemoryTimelockServices;
import com.palantir.timestamp.TimestampService;

public class InMemoryLockAndTimestampServiceFactory implements LockAndTimestampServiceFactory {
    private final InMemoryTimelockServices services;

    public InMemoryLockAndTimestampServiceFactory(InMemoryTimelockServices services) {
        this.services = services;
    }

    @Override
    public LockAndTimestampServices createLockAndTimestampServices() {
        TimestampService time = services.getTimestampService();
        LockService lock = services.getLockService();
        return ImmutableLockAndTimestampServices.builder()
                .lock(lock)
                .timestamp(time)
                .timestampManagement(services.getTimestampManagementService())
                // TODO(gs): pass client name from IMTS
                .timelock(new LegacyTimelockService(time, lock, LockClient.of("client")))
                .build();
    }
}
