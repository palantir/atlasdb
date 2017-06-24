/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.cleaner;

import java.util.function.Supplier;

import com.palantir.common.time.Clock;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.v2.TimelockService;

/**
 * Clock implementation that delegates to a LockService.
 *
 * @author jweel
 */
public final class GlobalClock implements Clock {
    private final Supplier<Long> timeSupplier;

    private GlobalClock(Supplier<Long> timeSupplier) {
        this.timeSupplier = timeSupplier;
    }

    public static GlobalClock create(RemoteLockService lockService) {
        return new GlobalClock(lockService::currentTimeMillis);
    }

    public static GlobalClock create(TimelockService timelock) {
        return new GlobalClock(timelock::currentTimeMillis);
    }

    @Override
    public long getTimeMillis() {
        return timeSupplier.get();
    }
}
