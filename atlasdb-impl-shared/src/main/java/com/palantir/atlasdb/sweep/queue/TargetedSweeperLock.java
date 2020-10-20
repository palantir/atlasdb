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
package com.palantir.atlasdb.sweep.queue;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.table.description.SweepStrategy.SweeperStrategy;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import java.util.Optional;

public final class TargetedSweeperLock {
    private final ShardAndStrategy shardStrategy;
    private final TimelockService timeLock;
    private final LockToken lockToken;

    private TargetedSweeperLock(ShardAndStrategy shardStrategy, TimelockService timeLock, LockToken lockToken) {
        this.shardStrategy = shardStrategy;
        this.timeLock = timeLock;
        this.lockToken = lockToken;
    }

    public static Optional<TargetedSweeperLock> tryAcquire(
            int shard, SweeperStrategy strategy, TimelockService timeLock) {
        ShardAndStrategy shardStrategy = ShardAndStrategy.of(shard, strategy);
        // We do not want the timeout to be too low to avoid a race condition where we give up too soon
        LockRequest request = LockRequest.of(ImmutableSet.of(shardStrategy.toLockDescriptor()), 100L);
        return timeLock.lock(request)
                .getTokenOrEmpty()
                .map(lockToken -> new TargetedSweeperLock(shardStrategy, timeLock, lockToken));
    }

    public ShardAndStrategy getShardAndStrategy() {
        return shardStrategy;
    }

    public void unlock() {
        timeLock.unlock(ImmutableSet.of(lockToken));
    }
}
