/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.timelock.lock;

import com.palantir.common.time.NanoTime;
import java.util.function.Supplier;

public class LeaseExpirationTimer {

    private volatile NanoTime lastRefreshTime;
    private final Supplier<NanoTime> clock;

    public LeaseExpirationTimer(Supplier<NanoTime> clock) {
        this.clock = clock;
        this.lastRefreshTime = clock.get();
    }

    public void refresh() {
        lastRefreshTime = clock.get();
    }

    public boolean isExpired() {
        return expiry().isBefore(clock.get());
    }

    public NanoTime lastRefreshTime() {
        return lastRefreshTime;
    }

    private NanoTime expiry() {
        return lastRefreshTime.plus(LockLeaseContract.SERVER_LEASE_TIMEOUT);
    }
}
