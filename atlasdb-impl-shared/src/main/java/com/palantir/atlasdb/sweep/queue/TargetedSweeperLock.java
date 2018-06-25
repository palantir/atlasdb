/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.queue;

import java.util.Optional;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;

public final class TargetedSweeperLock {
    private final ShardAndStrategy shardStrategy;
    private final TimelockService timeLock;
    private final LockToken lockToken;

    private TargetedSweeperLock(ShardAndStrategy shardStrategy, TimelockService timeLock, LockToken lockToken) {
        this.shardStrategy = shardStrategy;
        this.timeLock = timeLock;
        this.lockToken = lockToken;
    }

    public static Optional<TargetedSweeperLock> tryAcquire(int shard, TableMetadataPersistence.SweepStrategy strategy,
            TimelockService timeLock) {
        ShardAndStrategy shardStrategy = ShardAndStrategy.of(shard, strategy);
        LockDescriptor lock = StringLockDescriptor.of(shardStrategy.toText());
        LockRequest request = LockRequest.of(ImmutableSet.of(lock), 2000L);
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
