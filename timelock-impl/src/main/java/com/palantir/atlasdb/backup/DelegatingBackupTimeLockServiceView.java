/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.backup;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.timelock.BackupTimeLockServiceView;
import com.palantir.lock.v2.IdentifiedTimeLockRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.timestamp.TimestampManagementService;
import java.util.Set;

public class DelegatingBackupTimeLockServiceView implements BackupTimeLockServiceView {
    private final TimelockService timelockService;
    private final TimestampManagementService timestampManagementService;

    public DelegatingBackupTimeLockServiceView(
            TimelockService timelockService, TimestampManagementService timestampManagementService) {
        this.timelockService = timelockService;
        this.timestampManagementService = timestampManagementService;
    }

    @Override
    public long getFreshTimestamp() {
        return timelockService.getFreshTimestamp();
    }

    @Override
    public void fastForwardTimestamp(long currentTimestamp) {
        timestampManagementService.fastForwardTimestamp(currentTimestamp);
    }

    @Override
    public LockImmutableTimestampResponse lockImmutableTimestamp(IdentifiedTimeLockRequest _request) {
        return timelockService.lockImmutableTimestamp();
    }

    @Override
    public ListenableFuture<Set<LockToken>> unlock(Set<LockToken> tokens) {
        return Futures.immediateFuture(timelockService.unlock(tokens));
    }
}
