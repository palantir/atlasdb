/**
 * Copyright 2015 Palantir Technologies
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

import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.palantir.lock.LockClient;
import com.palantir.lock.RemoteLockService;
import com.palantir.timestamp.TimestampService;

/**
 * This will return immutableTimestamps
 * result.
 *
 * @author jweel
 *
 */
public class ImmutableTimestampSupplier implements Supplier<Long> {
    private static final long RELOAD_INTERVAL_MILLIS = 1000L;

    public static Supplier<Long> createMemoizedWithExpiration(RemoteLockService lockService,
                                                              TimestampService timestampService,
                                                              LockClient lockClient) {
        return Suppliers.memoizeWithExpiration(
                new ImmutableTimestampSupplier(lockService, timestampService, lockClient),
                RELOAD_INTERVAL_MILLIS,
                TimeUnit.MILLISECONDS);
    }

    private final RemoteLockService lockService;
    private final TimestampService timestampService;
    private final LockClient lockClient;

    private ImmutableTimestampSupplier(RemoteLockService lockService,
                                      TimestampService timestampService,
                                      LockClient lockClient) {
        Preconditions.checkArgument(lockClient != LockClient.ANONYMOUS);
        this.lockService = lockService;
        this.timestampService = timestampService;
        this.lockClient = lockClient;
    }

    @Override
    public Long get() {
        Long ts = timestampService.getFreshTimestamp();
        Long minLocked = lockService.getMinLockedInVersionId(lockClient.getClientId());
        return minLocked == null ? ts : minLocked;
    }
}
