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
package com.palantir.atlasdb.timelock.paxos;

import javax.ws.rs.QueryParam;

import com.google.common.base.Preconditions;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;

public class DelegatingManagedTimestampService implements ManagedTimestampService {
    private final TimestampService timestampService;
    private final TimestampManagementService timestampManagementService;

    public DelegatingManagedTimestampService(
            TimestampService timestampService,
            TimestampManagementService timestampManagementService) {
        Preconditions.checkNotNull(timestampService, "Timestamp service should not be null");
        Preconditions.checkNotNull(timestampManagementService, "Timestamp management service should not be null");
        this.timestampService = timestampService;
        this.timestampManagementService = timestampManagementService;
    }

    @Override
    public boolean isInitialized() {
        return timestampService.isInitialized();
    }

    @Override
    public long getFreshTimestamp() {
        return timestampService.getFreshTimestamp();
    }

    @Override
    public TimestampRange getFreshTimestamps(@QueryParam("number") int numTimestampsRequested) {
        return timestampService.getFreshTimestamps(numTimestampsRequested);
    }

    @Override
    public void fastForwardTimestamp(@QueryParam("currentTimestamp") long currentTimestamp) {
        timestampManagementService.fastForwardTimestamp(currentTimestamp);
    }

    @Override
    public String ping() {
        return timestampManagementService.ping();
    }
}
