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

import com.google.common.base.Suppliers;
import com.palantir.lock.v2.TimelockService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * This will return immutableTimestamps
 * result.
 *
 * @author jweel
 *
 */
public final class ImmutableTimestampSupplier {
    private static final long RELOAD_INTERVAL_MILLIS = 1000L;

    private ImmutableTimestampSupplier() {}

    public static Supplier<Long> createMemoizedWithExpiration(TimelockService timelockService) {
        return Suppliers.memoizeWithExpiration(
                timelockService::getImmutableTimestamp, RELOAD_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
    }
}
