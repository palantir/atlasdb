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
package com.palantir.atlasdb.cleaner;

import com.palantir.common.time.Clock;
import com.palantir.lock.LockService;
import com.palantir.lock.v2.TimelockService;
import java.util.function.Supplier;

/**
 * Clock implementation that delegates to a LockService.
 *
 * @author jweel
 */
public final class GlobalClock implements Clock {
    private final Supplier<Long> globalTimeSupplier;

    private GlobalClock(Supplier<Long> globalTimeSupplier) {
        this.globalTimeSupplier = globalTimeSupplier;
    }

    public static GlobalClock create(LockService lockService) {
        return new GlobalClock(lockService::currentTimeMillis);
    }

    public static GlobalClock create(TimelockService timelockService) {
        return new GlobalClock(timelockService::currentTimeMillis);
    }

    @Override
    public long getTimeMillis() {
        return globalTimeSupplier.get();
    }
}
