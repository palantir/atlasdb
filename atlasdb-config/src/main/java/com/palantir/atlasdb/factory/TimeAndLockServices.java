/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.factory;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.LockWatchCachingConfig;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManagerImpl;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.lock.client.BatchingCommitTimestampGetter;
import com.palantir.lock.client.LeaderTimeGetter;
import com.palantir.lock.client.LegacyLeaderTimeGetter;
import com.palantir.lock.client.LockLeaseService;
import com.palantir.lock.client.NamespacedConjureTimeLockServiceFactory;
import com.palantir.lock.client.NamespacedConjureTimelockService;
import com.palantir.lock.watch.LockWatchCache;
import com.palantir.lock.watch.LockWatchStarter;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import java.util.Optional;

public class TimeAndLockServices {
    private final BatchingCommitTimestampGetter batchingCommitTimestampGetter;

    public TimeAndLockServices(BatchingCommitTimestampGetter batchingCommitTimestampGetter) {
        this.batchingCommitTimestampGetter = batchingCommitTimestampGetter;
    }

    public static TimeAndLockServices create(
            ConjureTimelockService conjureTimelockService, String client, LockWatchStarter lockWatchStarter) {
        NamespacedConjureTimelockService namespacedConjureTimelockService =
                NamespacedConjureTimeLockServiceFactory.create(
                        conjureTimelockService, client, Optional.empty(), new DefaultTaggedMetricRegistry());
        LeaderTimeGetter leaderTimeGetter = new LegacyLeaderTimeGetter(namespacedConjureTimelockService);
        LockLeaseService lockLeaseService = LockLeaseService.create(namespacedConjureTimelockService, leaderTimeGetter);
        return new TimeAndLockServices(
                BatchingCommitTimestampGetter.create(lockLeaseService, lockWatchCache(lockWatchStarter)));
    }

    private static LockWatchCache lockWatchCache(LockWatchStarter lockWatchStarter) {
        return LockWatchManagerImpl.create(
                        MetricsManagers.createForTests(),
                        ImmutableSet.of(),
                        lockWatchStarter,
                        LockWatchCachingConfig.builder().build())
                .getCache();
    }

    public BatchingCommitTimestampGetter batchingCommitTimestampGetter() {
        return batchingCommitTimestampGetter;
    }
}
