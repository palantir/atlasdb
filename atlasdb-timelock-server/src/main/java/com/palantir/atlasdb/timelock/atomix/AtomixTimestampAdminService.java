/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.timelock.atomix;

import com.google.common.util.concurrent.Futures;
import com.palantir.timestamp.TimestampAdminService;

import io.atomix.variables.DistributedLong;

public class AtomixTimestampAdminService implements TimestampAdminService {
    private final DistributedLong timestamp;

    public AtomixTimestampAdminService(DistributedLong timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public void fastForwardTimestamp(long newMinimumTimestamp) {
        long currentTimestamp = Futures.getUnchecked(timestamp.get());
        while (currentTimestamp < newMinimumTimestamp) {
            if (Futures.getUnchecked(timestamp.compareAndSet(currentTimestamp, newMinimumTimestamp))) {
                return;
            }
            currentTimestamp = Futures.getUnchecked(timestamp.get());
        }
    }

    @Override
    public void invalidateTimestamps() {
        timestamp.set(Long.MIN_VALUE);
    }
}
