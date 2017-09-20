/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.PersistentTimestampService;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;

public class DbBoundManagedTimestampService implements ManagedTimestampService {
    private TimestampService timestampService;

    public DbBoundManagedTimestampService(TimestampService timestampService) {
        this.timestampService = timestampService;
    }

    @Override
    public long getFreshTimestamp() {
        return timestampService.getFreshTimestamp();
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        return timestampService.getFreshTimestamps(numTimestampsRequested);
    }

    @Override
    public void fastForwardTimestamp(long currentTimestamp) {
        if (PersistentTimestampService.class.isInstance(timestampService)) {
            ((PersistentTimestampService) timestampService).fastForwardTimestamp(currentTimestamp);
        } else if (InMemoryTimestampService.class.isInstance(timestampService)) {
            ((InMemoryTimestampService) timestampService).fastForwardTimestamp(currentTimestamp);
        } else {
            throw new UnsupportedOperationException("Cannot fastforward timestamp on DB store");
        }
    }

    @Override
    public String ping() {
        return PING_RESPONSE;
    }
}
