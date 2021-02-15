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
package com.palantir.timestamp;

import com.palantir.logsafe.Preconditions;
import javax.ws.rs.QueryParam;

public class DelegatingManagedTimestampService implements ManagedTimestampService {
    private final TimestampService timestampService;
    private final TimestampManagementService timestampManagementService;

    public DelegatingManagedTimestampService(
            TimestampService timestampService, TimestampManagementService timestampManagementService) {
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
