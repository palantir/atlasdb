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

import java.util.Optional;

import com.google.common.collect.ImmutableSortedMap;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockService;
import com.palantir.lock.StringLockDescriptor;

public final class TargetedSweeperLock {
    private final ShardAndStrategy shardStrategy;
    private final LockService lockService;
    private Optional<LockRefreshToken> lockRefreshToken;

    private TargetedSweeperLock(ShardAndStrategy shardStrategy, LockService lockService, LockRefreshToken token) {
        this.shardStrategy = shardStrategy;
        this.lockService = lockService;
        this.lockRefreshToken = Optional.ofNullable(token);
    }

    public static TargetedSweeperLock acquire(int shard, TableMetadataPersistence.SweepStrategy strategy,
            LockService lockService) throws InterruptedException {
        ShardAndStrategy shardStrategy = ShardAndStrategy.of(shard, strategy);
        LockDescriptor lock = StringLockDescriptor.of(shardStrategy.toText());
        LockRequest request = LockRequest.builder(ImmutableSortedMap.of(lock, LockMode.WRITE)).doNotBlock().build();
        LockRefreshToken token = lockService.lock(LockClient.ANONYMOUS.getClientId(), request);
        return new TargetedSweeperLock(shardStrategy, lockService, token);
    }

    public boolean isHeld() {
        return lockRefreshToken.isPresent();
    }

    public ShardAndStrategy getShardAndStrategy() {
        return shardStrategy;
    }

    public void unlock() {
        lockRefreshToken.ifPresent(lockService::unlock);
    }
}
